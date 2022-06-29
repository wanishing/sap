(ns build-shared)


(def app-name "sap")
(def resources-folder "resources/")
(def version (slurp (str resources-folder "VERSION")))
(def build-folder "target")
(def executable-folder (str build-folder "/executable"))
(def jar-content (str build-folder "/classes"))
(def uber-file (format "%s/%s-%s-standalone.jar" build-folder app-name version))
