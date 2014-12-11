(ns tag.apt.util
  (:import (java.io FileInputStream FileOutputStream BufferedInputStream BufferedOutputStream File)
           (java.util.zip GZIPInputStream GZIPOutputStream)
           (clojure.lang IDeref)
           (java.nio.channels FileChannel))
  (:require [clojure.java.io :as io]))

(defn pmapall
  "like pmap, but eager, and with more efficient core usage."
  [f coll]
  (let [num_threads (.. Runtime getRuntime availableProcessors)
        remaining   (atom ())
        func        (fn [item]
                      (let [result (f item)]
                        (swap! remaining next)
                        result))
        futures     (map #(future (func %)) coll)]
    (reset! remaining (drop num_threads futures))
    (first @remaining) ; run the futures we just dropped + 1 more
    (map deref futures)))

(defn pmapall-chunked
  "like pmap-chunked, but with pmapall"
  [n f coll]
  (apply concat
         (pmapall #(doall (map f %)) (partition-all n coll))))


(defn gz-reader
  ([file]
    (-> file io/as-file (FileInputStream.) (GZIPInputStream.) io/reader))
  ([file bufsz]
    (-> file io/as-file (FileInputStream.) (BufferedInputStream. bufsz) (GZIPInputStream.) io/reader)))

(defn gz-writer
  ([file]
    (-> file io/as-file (FileOutputStream.) (GZIPOutputStream.) io/writer))
  ([file bufsz]
    (-> file io/as-file (FileOutputStream.) (BufferedOutputStream. bufsz) (GZIPOutputStream.) io/writer)))


(defn- copy-file [^File from ^File to]
  (with-open [in (.getChannel (FileInputStream. from))
              out ^FileChannel (.getChannel (FileOutputStream. to))]
    (let [size (.size in)]
      (loop [offset 0]
        (prn offset size)
        (when (< offset size)
          (recur (+ offset (.transferFrom out in offset (- size offset)))))))))

(defn copy [^File from ^File to &{overwrite :overwrite :or {:overwrite true}}]
  (assert (.exists from) "can't copy nonexistent file bro")
  (if (.isDirectory from)
    (do
      (.mkdirs to)
      (doseq [child (.listFiles from)]
        (let [new-child (File. to (.getName child))]
          (copy child new-child))))
    (when (or overwrite (not (.exists to)))
      (copy-file from to))))

(defmacro lazy [& body]
  "returns a derefable object which, when dereferenced, evaluates body, then stores and returns
  the result such that subsequent dereferences return the same value"
  `(let [state# (atom ::unresolved)]
     (reify IDeref
       (deref [this#]
         (locking this#
           (let [val# @state#]
             (if (identical? val# ::unresolved)
               (reset! state# (do ~@body))
               val#)))))))