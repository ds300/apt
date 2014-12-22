(ns tag.apt.backend.leveldb-test
  (:import [uk.ac.susx.tag.apt ArrayAPT Util])
  (:require [tag.apt.backend :as b]
            [tag.apt.backend.leveldb :as leveldb]))

(defn stuff []
  (let [lexdesc (b/lexicon-descriptor "../apt-python/db")]
    (with-open [db (leveldb/from-descriptor lexdesc)]
      (println "yup" (.get db 1))
      (println (.sum (.fromByteArray ArrayAPT/factory (.get db 1))))
      (println (Util/base64encode (.get db 1))))))

