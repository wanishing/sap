;; see https://ask.clojure.org/index.php/10905/control-transient-deps-that-compiled-assembled-into-uberjar?show=10913#c10913
(require 'clojure.tools.deps.alpha.util.s3-transporter)


(ns build
  (:refer-clojure :exclude [compile])
  (:require
    [build-shared :as bs]
    [clojure.tools.build.api :as b]))


(def app-name bs/app-name)
(def version bs/version)
(def build-folder bs/build-folder)
(def jar-content bs/jar-content)
(def uber-file bs/uber-file) ; path for result uber file


(def uber-basis
  (b/create-basis {:project "deps.edn"
                   :aliases [:native-deps]}))


(defn uber
  [_]

  (b/copy-dir {:src-dirs   ["resources"]
               :target-dir jar-content})

  (println "Compiling Clojure sources to:" build-folder)

  (b/compile-clj {:basis uber-basis
                  :src-dirs ["src"]
                  :class-dir jar-content
                  :ns-compile '[sap.core]})
  (println "Done compiling Clojure sources.")

  (println "Building uberjar" uber-file)
  (b/uber {:class-dir jar-content
           :uber-file uber-file
           :basis uber-basis
           :main 'sap.core}))
