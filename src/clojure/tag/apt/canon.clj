(ns tag.apt.canon
  (:require [clojure.tools.logging :as log]))

(set! *warn-on-reflection* true)

(def ^:dynamic *pos-map*
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

(defn canonicalise-pos-tag [tag]
  (*pos-map* tag tag))


(def ^:dynamic *rel-map* (persistent! (reduce (fn [acc [base & _ :as things]]
                                                (reduce (fn [acc thing]
                                                          (assoc! acc
                                                                  thing base
                                                                  (str "_" thing) (str "_" base)))
                                                        acc
                                                        things))
                                  (transient {})
                                  [["root"]
                                   ["dep"]
                                   ["aux"
                                    "auxpass"
                                    "cop"]
                                   ["arg"
                                    "agent"
                                    "comp"
                                    "acomp"
                                    "ccomp"
                                    "xcomp"
                                    "pcomp"
                                    "obj"
                                    "dobj"
                                    "iobj"
                                    "pobj"
                                    "subj"
                                    "nsubj"
                                    "nsubjpass"
                                    "csubj"
                                    "csubjpass"]
                                   ["conj"]
                                   ["cc"]
                                   ["expl"]
                                   ["mod"
                                    "amod"
                                    "appos"
                                    "advcl"
                                    "det"
                                    "predet"
                                    "preconj"
                                    "vmod"
                                    "mwe"
                                    "mark"
                                    "advmod"
                                    "neg"
                                    "rcmod"
                                    "quantmod"
                                    "nn"
                                    "npadvmod"
                                    "tmod"
                                    "num"
                                    "number"
                                    "prep"
                                    "poss"
                                    "possessive"
                                    "prt"]
                                   ["parataxis"]
                                   ["punct"]
                                   ["ref"]
                                   ["sdep"
                                    "xsubj"]])))

(defn canonicalise-relation [^String rel]
  (or (*rel-map* rel)
      (do
        (log/warn
          "WARNING: unrecognized relation: " rel)
        (if (.startsWith rel "_")
          "_dep"
          "dep"))))
