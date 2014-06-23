(ns explorer.core
  (:require [clojure.string :refer [split]])
  (:gen-class))


(def ^:dynamic *lexicon* nil)
(def ^:dynamic *tkn-index* nil)
(def ^:dynamic *rel-index* nil)

(def last-tree (atom nil))


(defn load [id]
  (reset! last-tree (.get *lexicon* (.getIndex *tkn-index* id))))

(defn show []
  )

(defn exec [instr]
  (let [[cmd & args] (split instr #"\s")]
    (case cmd
      "show" (if (seq args)))))

(defn -main
  "I don't do a whole lot ... yet."
  [home dbname]
  )
