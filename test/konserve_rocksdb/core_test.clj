(ns konserve-rocksdb.core-test
  (:require [clojure.test :refer [deftest use-fixtures]]
            [konserve.compliance-test :refer [compliance-test]]
            [konserve.core :refer :all]
            [clojure.core.async :refer [<!! go chan]]
            [konserve-rocksdb.core :refer [new-rocksdb-store release]]
            [clojure.java.io])
  (:import [java.util
            UUID]))

(defn- delete-files-recursively [fname & [silently]]
  (letfn [(delete-f [file]
            (when (.isDirectory file)
              (doseq [child-file (.listFiles file)]
                (delete-f child-file)))
            (clojure.java.io/delete-file file silently))]
    (delete-f (clojure.java.io/file fname))))

(def ^:dynamic db nil)

(defn db-fixture [f]
  (let [db-dir (str "/tmp/clj-rocks-testdb-" (UUID/randomUUID) "/")]
    (with-redefs [db (<!! (new-rocksdb-store db-dir))]
      (f)
      (release db)
      (delete-files-recursively db-dir))))

(use-fixtures :each db-fixture)

(deftest rockdb-store
  (compliance-test db))
