(ns tag.apt.main
  (:gen-class)
  (:require [tag.apt.construct :as cons]
            [tag.apt.ppmi :as ppmi]
            [tag.apt.vectors :as vectors]
            [tag.apt.compose :as compose]))


(defn -main [command & args]
  (case command
    "construct" (apply cons/-main args)
    "ppmi" (apply ppmi/-main args)
    "compose" (apply compose/-main args)
    "vectors" (apply vectors/-main args)
    (throw (IllegalArgumentException. (str "unrecognised command: " command)))))
