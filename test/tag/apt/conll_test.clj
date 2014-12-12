(ns tag.apt.conll-test
  (:import (java.io StringReader))
  (:require [clojure.test :refer :all]
            [tag.apt.conll :refer [parse]]))

(def text "



0\t1\t2\t3
hello\tmy\tgood\tsir

this\tis\tthe
second\tsentence

\t\t
\t

blah
")

(def expected [
                [["0" "1" "2" "3"]
                 ["hello" "my" "good" "sir"]]

                [["this" "is" "the"]
                 ["second" "sentence"]]

                [["" "" ""] ["" ""]]

                [["blah"]]

                ])


(def text2
"

abcde\t1\t1
stuff\t2\t2
junk\t3\t3

jazz\t1
nothing

"
)

(def expected2
  [
    [[:edcba 1 2]
     [:ffuts 2 4]
     [:knuj 3 6]]

    [[:zzaj 1]
     [:gnihton]]
    ])

(deftest parse-test
  (is (= expected (parse (StringReader. text))))
  (is (= [[["   " "t"]]] (parse (StringReader. "   \tt"))))
  (is (= [[["   " "t" ""]]] (parse (StringReader. "   \tt\t"))))
  (is (not (seq (parse (StringReader. "")))))

  (is (= expected2 (parse (StringReader. text2)
                          [(comp keyword clojure.string/reverse)
                           #(Integer. %)
                           (comp #(* 2 %) #(Integer. %))]))))


