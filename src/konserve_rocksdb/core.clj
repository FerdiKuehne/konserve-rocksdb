(ns konserve-rocksdb.core
  (:require [clj-rocksdb :as rocks :refer [create-db destroy-db]]
            [clojure.core.async :as async :refer [<!! chan close! go put!]]
            [clojure.java.io :as io]
            [byte-streams :as bs]
            [hasch.core :refer [uuid]]
            [konserve
             [protocols :refer [PEDNAsyncKeyValueStore
                                -get -get-meta -update-in -dissoc -assoc-in
                                PBinaryAsyncKeyValueStore -bget -bassoc
                                -serialize -deserialize
                                PKeyIterable
                                -keys
                                -serialize -deserialize]]
             [serializers :refer [key->serializer byte->key serializer-class->byte]]
             [compressor :refer [byte->compressor compressor->byte lz4-compressor]]]
            [konserve.core :as k])
  (:import [java.io ByteArrayInputStream ByteArrayOutputStream]
           [java.nio ByteBuffer]))

(defn to-byte-array
  "Return 4 Byte Array with following content
     1th Byte = Version of Konserve
     2th Byte = Serializer Type
     3th Byte = Compressor Type
     4th Byte = Encryptor Type"
  [version serializer compressor encryptor]
  (let [env-array        (byte-array [version serializer compressor encryptor])
        return-buffer    (ByteBuffer/allocate 4)
        _                (.put return-buffer env-array)
        return-array     (.array return-buffer)]
    (.clear return-buffer)
    return-array))

(def version 1)

(def encrypt 0)

(defn write-edn [edn serializer compressor write-handlers]
  (let [bos           (ByteArrayOutputStream.)
        serializer-id (get serializer-class->byte (type serializer))
        compressor-id (get compressor->byte compressor)
        _             (.write bos (to-byte-array version serializer-id compressor-id encrypt))
        _             (-serialize (compressor serializer) bos write-handlers edn)]
    (.toByteArray bos)))

(defn read-edn [ba serializers read-handlers]
  (let [serializer-id  (aget ba 1)
        compressor-id  (aget ba 2)
        serializer-key (byte->key serializer-id)
        serializer     (get serializers serializer-key)
        compressor     (byte->compressor compressor-id)
        bis            (ByteArrayInputStream. ba 4 (count ba))
        edn            (-deserialize (compressor serializer) read-handlers bis)]
    [edn serializer]))

(defn exists? [rdb id]
  (let [it  (rocks/iterator rdb (bs/to-byte-array id))]
    (try
      (=
       (when-let [k (ffirst it)]
         (String. k))
       id)
      (catch java.util.NoSuchElementException e
        false)
      (catch Exception e
        (ex-info "Could not search for key."
                 {:type      :exist-error
                  :key       key
                  :exception e}))
      (finally
        (.close it)))))


(defrecord RocksDBStore [rdb serializers default-serializer compressor read-handlers write-handlers locks]
  PEDNAsyncKeyValueStore
  (-exists? [this key]
    ;; trick with iterator to not read values
    (let [res (chan)]
      (put! res (exists? rdb (str (uuid key) ".ksv")))
      res))

  (-get [this key] 
    (let [id  (str "v_" (uuid key) ".ksv")
          val (rocks/get rdb id)]
      (if (= val nil)
        (go nil)
        (let [res-ch (chan)]
          (try
            (let [res  (first (read-edn val serializers read-handlers))]
                (when res
                  (put! res-ch res)))
            (catch Exception e
              (put! res-ch (ex-info "Could not read key."
                                    {:type      :read-error
                                     :key       key
                                     :exception e})))
            (finally
              (close! res-ch)))
          res-ch))))

  (-get-meta [this key] 
    (let [id  (str (uuid key) ".ksv")
          val (rocks/get rdb id)]
      (if (= val nil)
        (go nil)
        (let [res-ch (chan)]
          (try
            (let [res  (first (read-edn val  serializers read-handlers))]
              (when res
                (put! res-ch res)))
            (catch Exception e
              (put! res-ch (ex-info "Could not read key."
                                    {:type      :read-meta-error
                                     :key       key
                                     :exception e})))
            (finally
              (close! res-ch)))
          res-ch))))

  (-assoc-in [this key-vec meta-up val]  (-update-in this key-vec meta-up (fn [_] val) []))

  (-update-in [this key-vec up-fn-meta up-fn up-fn-args]
    (let [[fkey & rkey] key-vec
          id            (str (uuid fkey) ".ksv")
          v-id          (str "v_" id)
          res-ch        (chan)]
      (try
        (let [[[old-meta _] [old-val serializer]] (if (exists? rdb id)
                                                    [(read-edn (rocks/get rdb id) serializers read-handlers) (read-edn (rocks/get rdb v-id) serializers read-handlers)]
                                                    [[nil] [nil (get serializers default-serializer)]])
              new-meta                            (up-fn-meta old-meta)
              new-val                             (if-not (empty? rkey)
                                                    (apply update-in old-val rkey up-fn up-fn-args)
                                                    (apply up-fn old-val up-fn-args)) 
              bam                                 (write-edn new-meta serializer compressor write-handlers)
              bav                                 (write-edn new-val serializer compressor write-handlers)]
          (rocks/put rdb
                     id    bam
                     v-id  bav)
          (put! res-ch [(get-in old-val rkey)
                        (get-in new-val rkey)]))
        (catch Exception e
          (put! res-ch (ex-info "Could not write key."
                                {:type      :write-error
                                 :key       fkey
                                 :exception e})))
        (finally
          (close! res-ch)))
        res-ch))

  (-dissoc [this key]
    (let [id     (str (uuid key) ".ksv")
          res-ch (chan)]
      (try
        (rocks/delete
         rdb
         (str id)
         (str "v_" id))
        (catch Exception e
          (put! res-ch (ex-info "Could not delete key."
                                {:type      :delete-error
                                 :key       key
                                 :exception e})))
        (finally
          (close! res-ch)))
      res-ch))

  PBinaryAsyncKeyValueStore
  (-bget [this key locked-cb]
    (let [id  (str (uuid key) ".ksv")
          val (rocks/get rdb id)]
      (if (nil? val)
        (go nil)
        (go
          (try
              (locked-cb {:input-stream  (ByteArrayInputStream. val)
                          :size         (count val)})
            (catch Exception e
              (ex-info "Could not read key."
                       {:type      :read-error
                        :key       key
                        :exception e})))))))

  (-bassoc [this key up-fn-meta input]
    (let [id   (str (uuid key) ".ksv")
          v-id (str "v_" (uuid key) ".ksv")]
      (go
        (try
          (let [[old-meta serializer] (if (exists? rdb id)
                                        (read-edn (rocks/get rdb id) serializers read-handlers)
                                        [nil (get serializers default-serializer)])
                new-meta              (up-fn-meta old-meta)
                bam                   (write-edn new-meta serializer compressor write-handlers)]
            (rocks/put rdb id bam)
            (rocks/put rdb v-id input))
          nil
          (catch Exception e
            (ex-info "Could not write key."
                     {:type      :write-error
                      :key       key
                      :exception e}))))))
  PKeyIterable
  (-keys [this]
    (let [iterator (rocks/iterator rdb)]
      (go
        (if (empty? iterator)
          #{}
          (try
            (reduce
             (fn [list-keys [n v]]
               (if (string? (re-find #"v_" (String. n)))
                 list-keys
                 (into list-keys [(first (read-edn v serializers read-handlers))])))
             #{}
             iterator)
            (catch Exception e
              {:msg :read-keys-error
               :e   e})
            (finally
              (.close iterator))))))))

(defn new-rocksdb-store
  [path & {:keys [rocksdb-opts default-serializer serializers compressor read-handlers write-handlers]
           :or   {default-serializer :FressianSerializer
                  compressor         lz4-compressor
                  read-handlers      (atom {})
                  write-handlers     (atom {})}}]
  (go (try
        (let [db (create-db path rocksdb-opts)]
          (map->RocksDBStore {:rdb db
                                        ;  :detect-old-version detect-old-version
                              :default-serializer default-serializer
                              :serializers        (merge key->serializer serializers)
                              :compressor         compressor
                              :read-handlers      read-handlers
                              :write-handlers     write-handlers
                              :locks              (atom {})}))
        (catch Exception e
          e))))


(defn release
  "Close the underlying RocksDB store. This cleanup is necessary as only one
  instance of RocksDB is allowed to work on a store at a time."
  [store]
  (.close (-> store :rdb)))

(defn delete-store
  "Removes store from disk."
  [path]
  (destroy-db path))

(comment

  (def store (<!! (new-rocksdb-store "/tmp/rocksdb2")))

  (get (:rdb store) (str (uuid "foo")))

  (:locks store)

  (<!! (k/get-in store ["foo"]))

  (<!! (k/exists? store "foo"))

  (first (<!! (k/assoc-in store 0 {:foo 42})))


  (<!! (k/keys store))

  (<!! (k/assoc store :foo2 {:baz 21}))

  (<!! (k/get-meta store "foo"))

  (<!! (k/get store "foo"))

  (<!! (k/update-in store ["foo" :foo] inc))

  (<!! (k/bget store ["foo"] (fn [arg]
                               (let [baos (ByteArrayOutputStream.)]
                                 (io/copy (:input-stream arg) baos)
                                 (prn "cb" (vec (.toByteArray baos)))))))

  (<!! (k/bassoc store ["foo"] (byte-array [42 42 42 42 42]))))
