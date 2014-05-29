(ns tag.apt.test.conll
  (:import (java.io StringReader))
  (:require [tag.apt.conll :refer [parse]])
  (:require [clojure.test :refer :all]))


(def text "



0\t1\t2\t3
hello\tmy\tgood\tsir

this\tis\tthe
second\tsentence

\t\t


blah
")

(def expected [
                [["0" "1" "2" "3"]
                 ["hello" "my" "good" "sir"]]

                [["this" "is" "the"]
                 ["second" "sentence"]]

                [["" ""]]

                [["blah"]]

                ])

(deftest parse-test
  (is (= expected (parse (StringReader. text))))
  (is (= [[["   " "t"]]] (parse (StringReader. "   \tt"))))
  (is (not (seq (parse (StringReader. ""))))))


