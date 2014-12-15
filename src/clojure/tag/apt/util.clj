(ns tag.apt.util
  (:import (java.io FileInputStream FileOutputStream BufferedInputStream BufferedOutputStream File ByteArrayOutputStream
                    ByteArrayInputStream)
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


(defn- copy-file
  "assumes `from` is a readable file that exists"
  [^File from ^File to]
  (with-open [in (.getChannel (FileInputStream. from))
              out ^FileChannel (.getChannel (FileOutputStream. to))]
    (let [size (.size in)]
      (loop [offset 0]
        (prn offset size)
        (when (< offset size)
          (recur (+ offset (.transferFrom out in offset (- size offset)))))))))

(defn copy
  "copies a file or folder from src to dst"
  [^File src ^File dst &{overwrite :overwrite :or {:overwrite true}}]
  (assert (.exists src) "can't copy nonexistent file bro")
  (if (.isDirectory src)
    (do
      (.mkdirs dst)
      (doseq [child (.listFiles src)]
        (let [new-child (File. dst (.getName child))]
          (copy child new-child))))
    (when (or overwrite (not (.exists dst)))
      (copy-file src dst))))

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

(defn compress
  "compresses a byte array using simple gzip, returning the compressed byte array."
  [^bytes bytes]
  (let [out (ByteArrayOutputStream. (alength bytes))]
    (doto (GZIPOutputStream. out)
      (.write bytes)
      (.flush)
      (.close))
    (.toByteArray out)))

(defn decompress
  "decompresses a gzipped byte array, returning the decompressed byte array"
  [^bytes bytes]
  (let [in (GZIPInputStream. (ByteArrayInputStream. bytes))
        out (ByteArrayOutputStream.)
        buf (byte-array 1024)]
    (loop [numBytes (.read in buf)]
      (when (> numBytes -1)
        (.write out buf 0 numBytes)
        (recur (.read in buf))))
    (.toByteArray out)))

(defprotocol Intable
  (to-int [val]))

(extend-type Number
  Intable
  (to-int [val] (.intValue val)))

(extend-type Integer
  Intable
  (to-int [val] val))

(extend-type String
  Intable
  (to-int [val] (Integer. val)))

(defn multiplex
  "multiplex seqs. like clojure.core/interleave but keeps going if the given seqs are different lengths"
  [& [s & more]]
  (when s
    (lazy-seq
      (if (seq s)
        (cons (first s) (apply multiplex (concat more (list (rest s)))))
        (apply multiplex more)))))