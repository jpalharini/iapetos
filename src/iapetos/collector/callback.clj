(ns iapetos.collector.callback
  (:require [iapetos.collector :as collector])
  (:import [io.prometheus.metrics.core.metrics
            CounterWithCallback$Callback
            GaugeWithCallback$Callback
            SummaryWithCallback$Callback]
           [io.prometheus.metrics.model.snapshots
            Quantiles
            Quantiles$Builder]
           [java.util.function
            Consumer]))

(defn labeled [callbacks labels]
  (map
   (fn [c]
     (update c :labels #(collector/ordered-labels labels %)))
   callbacks))

(defn counter-callback [callbacks]
  (reify Consumer
    (accept [_ callback]
      (doseq [{:keys [value-fn labels]} callbacks]
        (.call ^CounterWithCallback$Callback callback (value-fn) labels)))))

(defn gauge-callback [callbacks]
  (reify Consumer
    (accept [_ callback]
      (doseq [{:keys [value-fn labels]} callbacks]
        (.call ^GaugeWithCallback$Callback callback (value-fn) labels)))))

(defn- build-quantiles
  ^Quantiles [quantiles]
  (let [add-fn  (fn [b [q v]] (.quantile ^Quantiles$Builder b q v))
        builder ^Quantiles$Builder (reduce add-fn (Quantiles/builder) quantiles)]
    (.build builder)))

(defn summary-callback [callbacks]
  (reify Consumer
    (accept [_ callback]
      (doseq [{:keys [count-fn sum-fn quantiles-fn labels]} callbacks]
        (.call ^SummaryWithCallback$Callback callback
               (count-fn) (sum-fn)
               (build-quantiles (quantiles-fn))
               labels)))))