(ns tag.apt.main
  (:require [tag.apt.construct :as cons]
            [tag.apt.ppmi :as ppmi]))


(defn -main [command & args]
  (case command
    "construct" (apply cons/-main args)
    "ppmi" (apply ppmi/-main args)
    (throw (IllegalArgumentException. (str "unrecognised command: " command)))))
