(ns tag.apt.core-test
  (:import (uk.ac.susx.tag.apt Indexer))
  (:require [clojure.test :refer :all]
            [tag.apt.core :refer [indexer relation-indexer]]))

(deftest indexer-test
  (let [idx (indexer)]
    (is (= 0 (idx "zero")))
    (is (= 1 (idx "one")))
    (is (= 2 (idx "two")))
    (is (= 0 (idx "zero")))
    (is (= 1 (idx "one")))
    (is (= 2 (idx "two")))

    (is (= "zero" (idx 0)))
    (is (= "one" (idx 1)))
    (is (= "two" (idx 2)))

    (is (nil? (idx 234)))

    (is (= {"zero" 0 "one" 1 "two" 2} @idx)))

  (let [idx (indexer 5)]
    (is (= 5 (idx "five")))
    (is (= "five" (idx 5)))
    (is (= {"five" 5} @idx)))

  (let [idx (indexer {"seventy-two" 72 "thirty-four" 34 "twelfty" 120})]
    (is (= 120 (idx "twelfty")))
    (is (= "seventy-two" (idx 72)))
    (is (= 121 (idx "unseen")))
    (is (= "unseen" (idx 121)))

    (is (= @idx {"seventy-two" 72 "thirty-four" 34 "twelfty" 120 "unseen" 121}))))

(deftest relation-indexer-test
  (let [idx (relation-indexer)]
    (is (= 1 (idx "first")))
    (is (= 2 (idx "second")))
    (is (= "_first" (idx -1)))
    (is (= "first" (idx 1)))
    (is (= "_second" (idx -2)))
    (is (= "second" (idx 2)))
    (is (= -3 (idx "_third")))

    (is (nil? (idx -4)))

    (is (= {"first" 1 "second" 2 "third" 3} @idx)))

  (let [idx (relation-indexer {"amod" 4 "advmod" 7 "det" 2})]
    (is (= -4 (idx "_amod")))
    (is (= -7 (idx "_advmod")))
    (is (= -2 (idx "_det")))

    (is (= -8 (idx "_unseen")))

    (is (nil? (idx -1)))
    (is (nil? (idx 1)))

    (is (= {"amod" 4 "advmod" 7 "det" 2 "unseen" 8}))))
