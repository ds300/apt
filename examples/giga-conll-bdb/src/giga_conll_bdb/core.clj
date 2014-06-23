(ns giga-conll-bdb.core
  (:gen-class)
  (:import [uk.ac.susx.tag.apt AccumulativeAPTStore$Builder RGraph]
           [com.sleepycat.je EnvironmentConfig])
  (:require [tag.apt.backend.db :refer [bdb-lexicon *env-config*]]
            [tag.apt.util :refer [gz-reader pmapall]]
            [tag.apt.conll :refer [parse]]
            [clojure.pprint :refer [pprint]]))

(def pos-map
{  "JJ", "J"
"JJN", "J"
"JJS", "J"
"JJR", "J"
"VB", "V"
"VBD", "V"
"VBG", "V"
"VBN", "V"
"VBP", "V"
"VBZ", "V"
"NN", "N"
"NNS", "N"
"NNP", "N"
"NPS", "N"
"NP", "N"
"RB", "RB"
"RBR", "RB"
"RBS", "RB"
"DT", "DET"
"WDT", "DET"
"IN", "CONJ"
"CC", "CONJ"
"PRP", "PRON"
"PRP$", "PRON"
"WP", "PRON"
"WP$", "PRON"
".", "PUNCT"
",", "PUNCT"
":", "PUNCT"
";", "PUNCT"
"'", "PUNCT"
"\"", "PUNCT"
}
)

(def num-sents (atom 0))
(def last-report (atom (System/currentTimeMillis)))
(def last-n (atom 0))

(defn report! [n]
  (let [now (System/currentTimeMillis)
        time-diff (double (/ (- now @last-report) 1000))
        n-diff (- n @last-n)
        rate (/ n-diff time-diff)]
    (println "done " n " sents. Current rate: " rate "sents/s")
    (reset! last-report now)
    (reset! last-n n)))

(defn done-sent! []
  (let [n (swap! num-sents inc)]
    (when (= 0 (mod n 5000))
      (report! n))))

(defn to-graph [tkn-index dep-index sent]
  (let [numTokens (count sent)
        graph (RGraph. numTokens)
        ids   (.entityIds graph)]
    (doseq [[idx word lemma pos gov dep] sent]
      (when (and gov dep)
        (let [tkn-id (tkn-index (str (if (= pos "CD") "NUMBER" (.toLowerCase ^String lemma)) "/" (pos-map pos pos)))
              dep-id (dep-index dep)
              idx (Integer. idx)
              gov (Integer. gov)]
            (aset ids idx tkn-id)
            (.addRelation graph idx gov dep-id))))
    graph))

(def num-files (atom 0))

(defmacro nothrow [& body]
  `(try (do ~@body)
     (catch Throwable e# nil)))

(defn process-file! [lexicon file]
  (swap! num-files inc)
  (println "Processing file: " (.getAbsolutePath file))
  (let [tkn-index (.getEntityIndex lexicon)
        dep-index (.getRelationIndex lexicon)]
    (with-open [in (gz-reader file (.length file))]
      (doseq [sent (parse in)]
        (if-let [graph (nothrow (to-graph tkn-index dep-index sent))]
          (do
            (.include lexicon graph)
            (done-sent!))
          (do 
            (println "Failed to build graph for:")
            (pprint sent)))))))


(defn -main
  "I don't do a whole lot ... yet."
  [home dbname depth & input-files]
  (let [store-builder (.setMaxDepth (AccumulativeAPTStore$Builder.) (Integer. depth))]
    (binding [*env-config* (doto (EnvironmentConfig.)
                                (.setAllowCreate true)
                                (.setLocking false))]
    (with-open [lexicon (bdb-lexicon home dbname store-builder)]
      (dorun (pmapall (partial process-file! lexicon) (map clojure.java.io/as-file input-files)))
      (shutdown-agents)
      (report! @num-sents)
      (println "Number of files processed: " @num-files)))))
