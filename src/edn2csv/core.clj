(ns edn2csv.core
    (require [clojure.core.reducers :as r]
             [clojure.edn :as edn]
             [clojure.java.io :as io]
             [clojure.pprint :as pp]
             [iota]
             [me.raynes.fs :as fs])
    (:gen-class))

; Ignores (i.e., returns nil) any EDN entries that don't have the
; 'clojure/individual tag.
(defn individual-reader
    [t v]
    (when (= t 'clojush/individual) v))

; I got this from http://yellerapp.com/posts/2014-12-11-14-race-condition-in-clojure-println.html
; It prints in a way that avoids weird interleaving of lines and items.
; In several ways it would be better to use a CSV library like
; clojure.data.csv, but that won't (as written) avoid the interleaving
; problems, so I'm sticking with this approach for now.
(defn safe-println [output-stream & more]
    (.write output-stream (str (clojure.string/join "," more) "\n")))

(def semantics-atom (atom {}))

(defn new-uuid [] (str (java.util.UUID/randomUUID)))



; This prints out the relevant fields to the CSV filter
; and then returns 1 so we can count up how many individuals we processed.
; (The counting isn't strictly necessary, but it gives us something to
; fold together after we map this across the individuals; otherwise we'd
; just end up with a big list of nil's.)
(defn print-individual-to-csv
    [individual-out-file
     parentof-edges-out-file
     {:keys [uuid generation location, parent-uuids,
             genetic-operators, program, genome, total-error, errors], :as line}]
    ;(as-> line $
    ;      (map $ [:uuid :generation :location])
    ;      (concat $ ["Individual"])
    ;      (apply safe-println individual-out-file $))
    (safe-println individual-out-file uuid generation location "Individual")
    (swap! semantics-atom (partial merge-with into) {[total-error errors] [uuid]}) ; this feels wrong
    (doseq [parent parent-uuids] (safe-println parentof-edges-out-file parent genetic-operators uuid "PARENT_OF"))
    1)

(defn semantics-to-csv [semantics-out-file
                        errors-out-file
                        individual-semantics-edges-out-file
                        semantics-error-edges-out-file
                        [[total-error errors] uuids]]
    (let [semantic-uuid (new-uuid)] (safe-println semantics-out-file semantic-uuid total-error "Semantics")
                                    (doseq [ind-uuid uuids] (safe-println individual-semantics-edges-out-file ind-uuid semantic-uuid "HAS_SEMANTICS"))
                                    (doseq [[index item] (map-indexed vector errors)]
                                        (let [error-uid (new-uuid)] (safe-println errors-out-file error-uid item index "Error")
                                                                    (safe-println semantics-error-edges-out-file semantic-uuid error-uid "HAS_ERROR")))))


(defn build-csv-filename
    [edn-filename type]
    (str (fs/parent edn-filename)
         "/"
         (fs/base-name edn-filename ".edn")
         "_"
         type
         ".csv"))

(defn edn->csv-reducers [edn-file]
    (with-open [individual-out-file (io/writer (build-csv-filename edn-file "Individual"))
                semantics-out-file (io/writer (build-csv-filename edn-file "Semantics"))
                errors-out-file (io/writer (build-csv-filename edn-file "Errors"))
                parentof-edges-out-file (io/writer (build-csv-filename edn-file "ParentOf_edges"))
                individual-semantics-edges-out-file (io/writer (build-csv-filename edn-file "Individual_Semantics_edges"))
                semantics-error-edges-out-file (io/writer (build-csv-filename edn-file "Semantics_Error_edges"))]
        (safe-println individual-out-file "UUID:ID(Individual),Generation:int,Location:int,:LABEL")
        (safe-println semantics-out-file "UUID:ID(Semantics),TotalError:int,:LABEL")
        (safe-println errors-out-file "UUID:ID(Error),ErrorValue:int,Location:int,:LABEL")
        (safe-println parentof-edges-out-file ":START_ID(Individual),GeneticOperator,:END_ID(Individual),:TYPE")
        (safe-println individual-semantics-edges-out-file ":START_ID(Individual),:END_ID(Semantics),:TYPE")
        (safe-println semantics-error-edges-out-file "START_ID(Semantics),:END_ID(Error),:TYPE")
        (->>
            (iota/seq edn-file)
            (r/map (partial edn/read-string {:default individual-reader}))
            ; This eliminates empty (nil) lines, which result whenever
            ; a line isn't a 'clojush/individual. That only happens on
            ; the first line, which is a 'clojush/run, but we still need
            ; to catch it. We could do that with `r/drop`, but that
            ; totally kills the parallelism. :-(
            (r/filter identity)
            (r/map (partial print-individual-to-csv
                            individual-out-file
                            parentof-edges-out-file))
            (r/fold +))
        (doall (pmap (partial semantics-to-csv semantics-out-file ; this part is short and r/map doesn't work. Very little difference between map and pmap as well.
                              errors-out-file
                              individual-semantics-edges-out-file
                              semantics-error-edges-out-file)
                     @semantics-atom))))

(defn -main
    [edn-filename]
    (time
        (edn->csv-reducers edn-filename))
    ; Necessary to get threads spun up by `pmap` to shutdown so you get
    ; your prompt back right away when using `lein run`.
    (shutdown-agents))
