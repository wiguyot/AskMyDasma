const api = {
  status: "/api/status",
  services: "/api/services",
  currentCampaign: "/api/current-campaign",
  campaignProgress: "/api/campaign-progress",
  grafanaUrl: "/api/grafana-url",
  stop: "/api/campaigns/stop",
  reset: "/api/campaigns/reset",
  relaunch: "/api/campaigns/relaunch",
};

const els = {
  banner: document.querySelector("#message-banner"),
  lastRefreshed: document.querySelector("#last-refreshed"),
  overviewPlatform: document.querySelector("#overview-platform"),
  overviewCampaign: document.querySelector("#overview-campaign"),
  overviewAvailability: document.querySelector("#overview-availability"),
  overviewProblems: document.querySelector("#overview-problems"),
  prepareAgainButton: document.querySelector("#prepare-again-button"),
  stopButton: document.querySelector("#stop-button"),
  resetButton: document.querySelector("#reset-button"),
  refreshButton: document.querySelector("#refresh-button"),
  controlsReason: document.querySelector("#controls-reason"),
  campaignEmpty: document.querySelector("#campaign-empty"),
  campaignDetails: document.querySelector("#campaign-details"),
  campaignDataset: document.querySelector("#campaign-dataset"),
  campaignMode: document.querySelector("#campaign-mode"),
  campaignGroups: document.querySelector("#campaign-groups"),
  campaignFields: document.querySelector("#campaign-fields"),
  campaignValidationMode: document.querySelector("#campaign-validation-mode"),
  campaignValidationDetail: document.querySelector("#campaign-validation-detail"),
  campaignCreated: document.querySelector("#campaign-created"),
  campaignStarted: document.querySelector("#campaign-started"),
  campaignUpdated: document.querySelector("#campaign-updated"),
  campaignAdBody: document.querySelector("#campaign-ad-body"),
  campaignExplBody: document.querySelector("#campaign-expl-body"),
  campaignProgress: document.querySelector("#campaign-progress"),
  servicesBody: document.querySelector("#services-body"),
  log: document.querySelector("#log-output"),
  grafanaLink: document.querySelector("#grafana-link"),
  v2ForcedWarning: document.querySelector("#explainability-v2-forced-warning"),
};

const MAX_LOG_ENTRIES = 200;
const displayNames = {
  kafka1: "Kafka broker 1",
  kafka2: "Kafka broker 2",
  kafka3: "Kafka broker 3",
  "schema-registry": "Schema Registry",
  "init-topic": "Topic setup",
  producer: "Source Data producer",
  influxdb: "InfluxDB",
  "influxdb-init-buckets": "InfluxDB buckets setup",
  "influxdb-apply-template": "InfluxDB template",
  "askmydasma-web": "AskMyDasma backend",
  "hstree-analysis": "HSTree Analysis",
  "iforest-analysis": "IForest Analysis",
  "kitnet-analysis": "KitNET Analysis",
  "deepant-analysis": "DeepAnT Analysis",
  "mdragstream-point-analysis": "mDragStream Point-based Analysis",
  explainability: "Explainability",
  "explainability-v2": "Explainability v2",
  "explainability-shap-diff": "Explainability SHAP diff",
  "repeatability-v1": "Repeatability v1",
  "repeatability-v2": "Repeatability v2",
  "repeatability-diff": "Repeatability diff",
  "reproducibility-export": "Reproducibility export",
  "consensus-analysis": "Consensus analysis",
  "build-metrics": "Metrics builder",
  telegraf: "Telegraf",
  grafana: "Grafana",
};

const adModelOrder = ["hstree", "iforest", "kitnet", "deepant", "mdragpoint"];
const adDisplayNames = {
  hstree: "HSTree",
  iforest: "IForest",
  kitnet: "KitNET",
  deepant: "DeepAnT",
  mdragpoint: "mDragStream Point-based",
};

let refreshTimer = null;
let backgroundRefreshInFlight = null;
let actionInFlight = false;
let lastCampaign = null;
let lastStatus = null;
let lastServices = null;
let lastRefresh = null;
let logEntries = [];

function displayName(value) {
  return displayNames[value] || value || "-";
}

function escapeHtml(value) {
  return String(value ?? "")
    .replaceAll("&", "&amp;")
    .replaceAll("<", "&lt;")
    .replaceAll(">", "&gt;")
    .replaceAll('"', "&quot;")
    .replaceAll("'", "&#39;");
}

function textOrDash(value) {
  return value === null || value === undefined || value === "" ? "—" : String(value);
}

function log(message) {
  const stamp = new Date().toISOString();
  logEntries.unshift(`[${stamp}] ${message}`);
  if (logEntries.length > MAX_LOG_ENTRIES) {
    logEntries = logEntries.slice(0, MAX_LOG_ENTRIES);
  }
  els.log.textContent = logEntries.join("\n");
}

function showBanner(message, kind = "info") {
  els.banner.textContent = message;
  els.banner.className = `banner banner-${kind}`;
}

function prettyLabel(value) {
  if (!value) {
    return "-";
  }
  const labels = {
    realtime: "Realtime 1 msg/s",
    "realtime-x5": "Realtime 5 msg/s",
    "realtime-x20": "Realtime 20 msg/s",
    "realtime-x100": "Realtime 100 msg/s",
    batch: "Batch",
    not_configured: "not configured",
    unavailable: "unavailable",
    reseted: "reset",
    resetting: "resetting",
    waiting_for_launch: "waiting for launch",
    preparing_platform: "preparing the platform",
    processing_data: "platform processing data",
    stopping_platform: "stopping the platform",
    resetting_platform: "resetting the platform",
    ready_for_production: "ready for production",
    configuring: "configuring",
    starting: "starting",
    running: "running",
    stopping: "stopping",
    idle: "idle",
    error: "error",
  };
  return labels[value] || String(value).replaceAll("_", " ");
}

async function fetchJson(url, options = {}) {
  const response = await fetch(url, options);
  if (!response.ok) {
    let detail = `${response.status} ${response.statusText}`;
    try {
      const payload = await response.json();
      if (payload.detail) {
        if (typeof payload.detail === "object") {
          throw new ActionBlockedError(payload.detail);
        }
        detail = payload.detail;
      }
    } catch (error) {
      if (error instanceof ActionBlockedError) {
        throw error;
      }
    }
    throw new Error(detail);
  }
  return response.json();
}

class ActionBlockedError extends Error {
  constructor(detail) {
    super(detail?.message || `Request blocked (${detail?.state || "unknown"})`);
    this.blocked = true;
    this.state = detail?.state || null;
    this.reason = detail?.reason || null;
  }
}

// ---------------------------------------------------------------------------
// Effective configuration tables
// ---------------------------------------------------------------------------

function fmtBool(value) {
  return value ? "yes" : "no";
}

function hstreeEstimatorDetails(c) {
  if (c.hstree_estimator === "p99") {
    return [{ "P99 window": c.hstree_threshold_window }];
  }
  const rows = [
    { "K sigma": c.hstree_anomaly_k_sigma, "Warmup window": c.hstree_anomaly_warmup_window },
  ];
  if (c.hstree_estimator === "welford-windowed") {
    rows.push({ "Welford window": c.hstree_welford_window });
  }
  return rows;
}

function iforestEstimatorDetails(c) {
  if (c.iforest_estimator === "native") {
    return null;
  }
  if (c.iforest_estimator === "p99") {
    return [{ "P99 window": c.iforest_threshold_window }];
  }
  const rows = [
    { "K sigma": c.iforest_anomaly_k_sigma, "Warmup window": c.iforest_anomaly_warmup_window },
  ];
  if (c.iforest_estimator === "welford-windowed") {
    rows.push({ "Welford window": c.iforest_welford_window });
  }
  return rows;
}

function kitnetEstimatorDetails(c) {
  if (c.kitnet_estimator === "p99") {
    return [{ "P99 window": c.kitnet_threshold_window }];
  }
  const rows = [
    { "K sigma": c.kitnet_anomaly_k_sigma, "Warmup window": c.kitnet_anomaly_warmup_window },
  ];
  if (c.kitnet_estimator === "welford-windowed") {
    rows.push({ "Welford window": c.kitnet_welford_window });
  }
  return rows;
}

function deepantEstimatorDetails(c) {
  if (c.deepant_estimator === "p99") {
    return [
      { "P99 window": c.deepant_threshold_window },
      { Percentile: c.deepant_threshold_percentile },
    ];
  }
  const rows = [
    { "K sigma": c.deepant_anomaly_k_sigma, "Warmup window": c.deepant_anomaly_warmup_window },
  ];
  if (c.deepant_estimator === "welford-windowed") {
    rows.push({ "Welford window": c.deepant_welford_window });
  }
  return rows;
}

function mdragpointEstimatorDetails(c) {
  if (c.mdragpoint_verdict_mode !== "external") {
    return null;
  }
  const rows = [];
  if (c.mdragpoint_estimator === "p99") {
    rows.push({ "P99 window": c.mdragpoint_threshold_window }, { Percentile: c.mdragpoint_threshold_percentile });
  } else {
    rows.push({ "K sigma": c.mdragpoint_anomaly_k_sigma, "Warmup window": c.mdragpoint_anomaly_warmup_window });
    if (c.mdragpoint_estimator === "welford-windowed") {
      rows.push({ "Welford window": c.mdragpoint_welford_window });
    }
  }
  return rows;
}

function hstreeCoreParams(c) {
  return [
    { "Initial window": c.hstree_initial_window_size },
    { "Window size": c.hstree_window_size },
    { "Num trees": c.hstree_num_trees },
    { Variant: c.hstree_variant === "paper" ? "Paper (Tan et al., 2011)" : "PySAD" },
    { "Max depth": c.hstree_max_depth },
  ];
}

function iforestCoreParams(c) {
  return [
    { "Initial window": c.iforest_initial_window_size },
    { "Window size": c.iforest_window_size },
    { "N estimators": c.iforest_n_estimators },
    { "Max samples": c.iforest_max_samples },
    { "Refit every": c.iforest_refit_every },
    { Contamination: c.iforest_contamination === "auto" ? "Auto" : c.iforest_contamination },
  ];
}

function kitnetCoreParams(c) {
  return [
    { "Max AE size": c.kitnet_max_size_ae },
    { "Grace FM": c.kitnet_grace_feature_mapping },
    { "Grace AD": c.kitnet_grace_anomaly_detector },
    { "Learning rate": c.kitnet_learning_rate },
    { "Hidden ratio": c.kitnet_hidden_ratio },
    { Clip: c.kitnet_clip },
  ];
}

function deepantCoreParams(c) {
  return [
    { Lookback: c.deepant_lookback_size },
    { "Initial window": c.deepant_initial_window_size },
    { "Refit every": c.deepant_refit_every },
    { "Window size": c.deepant_window_size },
    { Filters: c.deepant_filters },
    { Kernel: c.deepant_kernel_size },
    { Dropout: c.deepant_dropout },
    { LR: c.deepant_lr },
    { "Epochs init": c.deepant_epochs_initial },
    { "Epochs refit": c.deepant_epochs_refit },
    { Batch: c.deepant_batch_size },
    { "Max workers": c.deepant_max_group_workers },
  ];
}

function mdragpointCoreParams(c) {
  return [
    { "Dynamic threshold": fmtBool(c.mdragpoint_dynamic_threshold) },
    { "LOF init": fmtBool(c.mdragpoint_lof_init) },
    { "N init": c.mdragpoint_n_init },
    { Clusters: c.mdragpoint_max_nb_clusters },
    { "Cluster size": c.mdragpoint_max_cluster_size },
    { "LOF neighbors": c.mdragpoint_lof_neighbors },
  ];
}

function paramPairsToHtml(rows) {
  if (!rows) {
    return '<span class="param-empty">—</span>';
  }
  const flat = rows.flatMap((row) => Object.entries(row));
  return `<div class="param-list">${flat
    .map(([key, value]) => `<div class="param-list-row"><span class="param-list-key">${escapeHtml(key)}</span><span>${escapeHtml(textOrDash(value))}</span></div>`)
    .join("")}</div>`;
}

function renderAdRow(model, c) {
  const estimatorLabels = {
    native: "Native (Isolation Forest)",
    p99: "P99 sliding window",
    welford: "Welford (K×σ)",
    "welford-windowed": "Welford windowed (K×σ)",
  };
  const estimatorValue =
    model === "mdragpoint"
      ? c.mdragpoint_verdict_mode === "external"
        ? estimatorLabels[c.mdragpoint_estimator]
        : "Native (internal threshold)"
      : estimatorLabels[c[`${model}_estimator`]];
  const estimatorDetails =
    model === "hstree" ? hstreeEstimatorDetails(c)
      : model === "iforest" ? iforestEstimatorDetails(c)
        : model === "kitnet" ? kitnetEstimatorDetails(c)
          : model === "deepant" ? deepantEstimatorDetails(c)
            : mdragpointEstimatorDetails(c);
  const coreParams =
    model === "hstree" ? hstreeCoreParams(c)
      : model === "iforest" ? iforestCoreParams(c)
        : model === "kitnet" ? kitnetCoreParams(c)
          : model === "deepant" ? deepantCoreParams(c)
            : mdragpointCoreParams(c);
  return `
    <tr>
      <td data-label="Model"><details class="admin-row-details"><summary>${escapeHtml(adDisplayNames[model])}</summary></details></td>
      <td data-label="Anomaly estimator">${escapeHtml(textOrDash(estimatorValue))}</td>
      <td data-label="Estimator parameters">${paramPairsToHtml(estimatorDetails)}</td>
      <td data-label="Core parameters"><details class="admin-row-details"><summary class="admin-params-summary">Show parameters</summary>${paramPairsToHtml(coreParams)}</details></td>
    </tr>`;
}

function renderAdTable(c) {
  const selected = new Set(c.selected_models || []);
  els.campaignAdBody.innerHTML = adModelOrder
    .filter((model) => selected.has(model))
    .map((model) => renderAdRow(model, c))
    .join("");
}

function explainabilityModeLabel(mode) {
  if (mode === "simple") return "Independent";
  if (mode === "reference") return "Check against detector";
  return null;
}

function explMethodLabel(method) {
  if (method === "tree") return "TreeExplainer (iForest path length)";
  if (method === "kernel") return "KernelSHAP";
  return null;
}

function renderExplRow(serviceName, service) {
  const model = service.model ? adDisplayNames[service.model] || service.model : null;
  const mode = explainabilityModeLabel(service.mode);
  const isKernel = service.method === "kernel";
  const parameters = !service.method
    ? "—"
    : isKernel
      ? `SHAP samples: ${service.nsamples ?? "-"}${service.model === "kitnet" ? " · Very high compute cost" : ""}`
      : "—";
  return `
    <tr>
      <td data-label="Service">${escapeHtml(serviceName)}</td>
      <td data-label="Model to explain">${escapeHtml(textOrDash(model))}</td>
      <td data-label="Mode">${escapeHtml(textOrDash(mode))}</td>
      <td data-label="Method">${escapeHtml(textOrDash(service.method ? explMethodLabel(service.method) : null))}</td>
      <td data-label="Parameters">${escapeHtml(parameters)}</td>
    </tr>`;
}

function renderExplTable(c) {
  const rows = [];
  if (c.explainability_model || c.explainability_mode || c.explainability_method) {
    rows.push(renderExplRow("Explainability", {
      model: c.explainability_model, mode: c.explainability_mode,
      method: c.explainability_method || "kernel", nsamples: c.explainability_nsamples,
    }));
  }
  if (c.explainability_v2_model || c.explainability_v2_mode || c.explainability_v2_method) {
    rows.push(renderExplRow("Explainability v2", {
      model: c.explainability_v2_model, mode: c.explainability_v2_mode,
      method: c.explainability_v2_method || "kernel", nsamples: c.explainability_v2_nsamples,
    }));
  }
  els.campaignExplBody.innerHTML = rows.join("") || '<tr><td colspan="5">No explainability service selected.</td></tr>';
}

function renderCampaign(c) {
  if (!c) {
    els.campaignEmpty.classList.remove("is-hidden");
    els.campaignDetails.classList.add("is-hidden");
    return;
  }
  els.campaignEmpty.classList.add("is-hidden");
  els.campaignDetails.classList.remove("is-hidden");
  els.campaignDataset.textContent = c.dataset_path || "—";
  els.campaignMode.textContent = prettyLabel(c.mode);
  els.campaignGroups.textContent = c.selected_groups?.length ? c.selected_groups.join(", ") : "—";
  els.campaignFields.textContent = c.excluded_fields?.length ? c.excluded_fields.join(", ") : "—";

  const repeatability = adModelOrder.filter((m) => c[`${m}_repeatability`]);
  const reproducibility = adModelOrder.filter((m) => c[`${m}_reproducibility`]);
  let validationMode = "None";
  let validationDetail = "—";
  if (repeatability.length) {
    validationMode = "Repeatability";
    validationDetail = `${adDisplayNames[repeatability[0]]}`;
  } else if (reproducibility.length) {
    validationMode = "Reproducibility";
    validationDetail = `${adDisplayNames[reproducibility[0]]} · role ${c.reproducibility_source_role || "—"} · campaign ${c.reproducibility_campaign_id || "—"}`;
  }
  els.campaignValidationMode.textContent = validationMode;
  els.campaignValidationDetail.textContent = validationDetail;
  els.campaignCreated.textContent = textOrDash(c.created_at);
  els.campaignStarted.textContent = textOrDash(c.started_at);
  els.campaignUpdated.textContent = textOrDash(c.updated_at);
  renderAdTable(c);
  renderExplTable(c);
}

// ---------------------------------------------------------------------------
// Services table
// ---------------------------------------------------------------------------

const serviceStateLabels = {
  running: "Running",
  healthy: "Running",
  stopped: "Stopped",
  starting: "Starting",
  not_started: "Not started",
  unhealthy: "Unhealthy",
  error: "Error",
  unknown: "Unknown",
  exited: "Stopped",
  dead: "Error",
};

function serviceStateLabel(service) {
  if (service.state === "running" && service.health === "unhealthy") {
    return "Unhealthy";
  }
  return serviceStateLabels[service.state] || serviceStateLabels[service.status] || "Unknown";
}

function serviceStateBadgeClass(service) {
  const label = serviceStateLabel(service);
  if (label === "Running") return "healthy";
  if (label === "Unhealthy" || label === "Error") return "not-running";
  if (label === "Starting") return "starting";
  if (label === "Disabled") return "idle";
  return "unknown";
}

function isServiceProblem(service, processing) {
  if (service.status === "error" || service.state === "dead") return true;
  if (service.state === "running" && service.health === "unhealthy") return true;
  if (service.expected !== true) return false;
  if (service.permanent === false) return false; // one-shot completed successfully
  if (!processing) return false;
  return service.state === "not_started" || service.state === "exited" || service.state === "stopped";
}

function renderServices(services, status, campaign) {
  const processing = status?.platform_status === "processing_data";
  const groups = new Map();
  for (const service of services) {
    const group = service.group || "Infrastructure";
    if (!groups.has(group)) {
      groups.set(group, []);
    }
    groups.get(group).push(service);
  }

  const rows = [];
  for (const [group, groupServices] of groups) {
    rows.push(`<tr class="service-group-row"><td colspan="5">${escapeHtml(group)}</td></tr>`);
    for (const service of groupServices) {
      const expected = service.expected === null || service.expected === undefined
        ? "—"
        : service.expected ? "Yes" : "No";
      const problem = isServiceProblem(service, processing);
      rows.push(`
        <tr class="${problem ? "service-problem" : ""}">
          <td data-label="Service">${escapeHtml(displayName(service.name))}</td>
          <td data-label="Role">${escapeHtml(textOrDash(service.role))}</td>
          <td data-label="Expected">${escapeHtml(expected)}</td>
          <td data-label="State"><span class="badge ${serviceStateBadgeClass(service)}">${escapeHtml(serviceStateLabel(service))}</span></td>
          <td data-label="Health">${escapeHtml(textOrDash(service.health))}</td>
        </tr>`);
    }
  }
  els.servicesBody.innerHTML = rows.join("") || '<tr><td colspan="5">No service observed.</td></tr>';

  const permanent = services.filter((s) => s.expected === true && s.permanent !== false);
  const available = permanent.filter((s) => s.state === "running");
  els.overviewAvailability.textContent = permanent.length
    ? `${available.length}/${permanent.length}`
    : "Unknown";
  const problems = services.filter((s) => isServiceProblem(s, processing));
  els.overviewProblems.textContent = problems.length
    ? problems.map((s) => displayName(s.name)).join(", ")
    : "None";
  return problems;
}

// ---------------------------------------------------------------------------
// Progress
// ---------------------------------------------------------------------------

function renderCampaignProgress(progress) {
  const items = progress?.items || [];
  if (!items.length) {
    els.campaignProgress.innerHTML = '<p class="progress-empty">No progress available.</p>';
  } else {
    els.campaignProgress.innerHTML = items
      .map((item) => {
        const produced = item.produced_records ?? 0;
        const total = item.total_records ?? 0;
        const hasTotal = total > 0;
        const percent = hasTotal && item.progress_percent != null
          ? `${Number(item.progress_percent).toFixed(1)} %`
          : "—";
        return `
          <article class="progress-item">
            <div class="progress-item-head">
              <strong>${escapeHtml(item.label)}</strong>
              <span>${escapeHtml(percent)}</span>
            </div>
            <div class="progress-item-meta">
              <span>${produced} / ${hasTotal ? total : "—"} records</span>
              <span>Stream: ${escapeHtml(displayName(item.topic))}</span>
            </div>
          </article>`;
      })
      .join("");
  }
  renderExplainabilityV1Progress(progress?.explainability_v1);
  renderExplainabilityV2Progress(progress?.explainability_v2);
}

function renderExplainabilityV1Progress(view) {
  renderExplainabilityProgress("v1", view);
}

function renderExplainabilityV2Progress(view) {
  renderExplainabilityProgress("v2", view);
}

function renderExplainabilityProgress(version, view) {
  const card = document.querySelector(`#explainability-${version}-progress`);
  if (!card) return;
  if (!view || !view.selected) {
    card.classList.add("is-hidden");
    return;
  }
  const state = view.state || "processing";
  const stateLabels = { processing: "Processing", complete: "Complete", failed: "Failed" };
  const stateElement = document.querySelector(`#explainability-${version}-state`);
  stateElement.textContent = stateLabels[state] || "Unknown";
  stateElement.className = `badge v2-state-${state}`;
  card.classList.remove("is-hidden");
  document.querySelector(`#explainability-${version}-lag`).textContent =
    view.consumer_lag == null ? "Consumer lag: unknown" : `Consumer lag: ${view.consumer_lag}`;
  const recordsElement = document.querySelector(`#explainability-${version}-records`);
  if (view.published_records != null && view.upstream_records != null) {
    recordsElement.textContent = `Published: ${view.published_records} / ${view.upstream_records}`;
  } else {
    recordsElement.textContent = "Published: unknown";
  }
  document.querySelector(`#explainability-${version}-reason`).textContent = view.reason || "-";
}

// ---------------------------------------------------------------------------
// Commands
// ---------------------------------------------------------------------------

function processingUnderway(status, services) {
  if (status?.platform_status === "processing_data") {
    return true;
  }
  const runtimeGroups = ["Detectors", "Explainability", "Validation"];
  return services.some(
    (s) => runtimeGroups.includes(s.group) && s.expected === true && s.state === "running",
  );
}

function platformTransitioning(status) {
  return ["preparing_platform", "stopping_platform", "resetting_platform", "starting"].includes(
    status?.platform_status || "",
  );
}

function dataIsFresh() {
  return lastStatus !== null && lastServices !== null;
}

function updateCommands(status, services, campaign) {
  const busy = actionInFlight || platformTransitioning(status);
  const processing = processingUnderway(status, services);
  const stopped = !processing;
  const hasCampaign = Boolean(campaign);
  const fresh = dataIsFresh();

  els.stopButton.disabled = busy || !fresh || !processing;
  els.resetButton.disabled = busy || !fresh || processing;
  els.prepareAgainButton.disabled = busy || !fresh || !hasCampaign || processing;
  els.refreshButton.disabled = actionInFlight;

  const reasons = [];
  if (!fresh) reasons.push("Data unavailable or outdated: commands are disabled until a refresh succeeds.");
  else if (busy) reasons.push("A platform operation is in progress.");
  else if (processing) reasons.push("Campaign processing is running: stop it before resetting or preparing again.");
  else if (!hasCampaign) reasons.push("No current campaign: use Configure campaign in the user workspace.");
  els.controlsReason.textContent = reasons.join(" ") || "Commands ready.";
}

async function postAction(url, label) {
  setBusyState(true);
  showBanner(`${label} in progress...`, "info");
  log(`${label} requested.`);
  try {
    const result = await fetchJson(url, { method: "POST", headers: { "Content-Type": "application/json" } });
    log(`${label}: OK`);
    showBanner(`${label} completed.`, "success");
    await refreshAll();
    return result;
  } finally {
    setBusyState(false);
  }
}

async function postActionWithForceGuard(url, label) {
  try {
    return await postAction(url, label);
  } catch (error) {
    if (!error?.blocked) {
      throw error;
    }
    const warning = `Explainability is ${error.state || "processing"}: ${error.reason || "completion cannot be confirmed"}.`;
    showBanner(warning, "error");
    log(`${label} blocked: ${warning}`);
    const confirmed = window.confirm(
      `${warning}\n\nForce this ${label.toLowerCase()} action anyway? ` +
        "The campaign will remain incomplete and results may be missing.",
    );
    if (!confirmed) {
      showBanner(`${label} cancelled: the campaign remains incomplete.`, "error");
      log(`${label} cancelled by the user; no request sent.`);
      return null;
    }
    const forcedResult = await postAction(`${url}?force=true`, `${label} (forced)`);
    if (els.v2ForcedWarning) {
      els.v2ForcedWarning.classList.remove("is-hidden");
    }
    return forcedResult;
  }
}

function setBusyState(isBusy) {
  actionInFlight = isBusy;
  if (lastStatus !== null) {
    updateCommands(lastStatus, lastServices, lastCampaign);
  }
}

// ---------------------------------------------------------------------------
// Refresh loop
// ---------------------------------------------------------------------------

async function refreshAll() {
  const [status, campaign, services, grafana, progress] = await Promise.all([
    fetchJson(api.status),
    fetchJson(api.currentCampaign),
    fetchJson(api.services),
    fetchJson(api.grafanaUrl),
    fetchJson(api.campaignProgress),
  ]);

  lastStatus = status;
  lastCampaign = campaign;
  lastServices = services;
  lastRefresh = new Date();

  els.grafanaLink.href = grafana.url || "http://localhost:3000";
  els.overviewPlatform.textContent = prettyLabel(status.platform_status);
  els.overviewCampaign.textContent = prettyLabel(campaign?.status || "idle");
  els.lastRefreshed.textContent = `Last update: ${lastRefresh.toLocaleTimeString("en-GB")}`;

  renderCampaign(campaign);
  const problems = renderServices(services, status, campaign);
  renderCampaignProgress(progress);
  updateCommands(status, services, campaign);

  if (!actionInFlight) {
    if (problems.length > 0) {
      showBanner(`Problems detected: ${problems.map((s) => displayName(s.name)).join(", ")}`, "error");
    } else if (status.platform_status === "processing_data") {
      showBanner("Campaign is running.", "success");
    } else {
      showBanner("Backend ready.", "info");
    }
  }
}

// ---------------------------------------------------------------------------
// Actions
// ---------------------------------------------------------------------------

els.stopButton.addEventListener("click", async () => {
  try {
    await postActionWithForceGuard(api.stop, "Stop campaign");
  } catch (error) {
    showBanner(error.message, "error");
    log(`Stop campaign failed: ${error.message}`);
  }
});

els.resetButton.addEventListener("click", async () => {
  const confirmed = window.confirm(
    "Reset campaign data?\n\n" +
      "This recreates Kafka topics (all messages are deleted), removes Schema Registry subjects " +
      "and resets the InfluxDB storage of the whole stack (buckets, dashboards data and volumes).\n\n" +
      "It does not only clear the campaign summary. Continue?",
  );
  if (!confirmed) {
    log("Reset cancelled by the user; no request sent.");
    showBanner("Reset cancelled; no data was modified.", "info");
    return;
  }
  try {
    await postActionWithForceGuard(api.reset, "Reset campaign data");
  } catch (error) {
    showBanner(error.message, "error");
    log(`Reset failed: ${error.message}`);
  }
});

els.prepareAgainButton.addEventListener("click", async () => {
  const confirmed = window.confirm(
    "Prepare again?\n\n" +
      "This re-runs the preparation of the saved campaign (topics and runtime images). " +
      "It does not start production automatically: use Start analysis in the user workspace when ready.",
  );
  if (!confirmed) {
    log("Prepare again cancelled by the user; no request sent.");
    return;
  }
  try {
    await postAction(api.relaunch, "Prepare again");
  } catch (error) {
    showBanner(error.message, "error");
    log(`Prepare again failed: ${error.message}`);
  }
});

els.refreshButton.addEventListener("click", async () => {
  try {
    await refreshAll();
    log("Status refreshed.");
  } catch (error) {
    showBanner(error.message, "error");
    log(`Refresh failed: ${error.message}`);
  }
});

// ---------------------------------------------------------------------------
// Polling: 5 s cadence, no overlapping requests, paused when hidden
// ---------------------------------------------------------------------------

const POLL_DELAY_MS = 5000;

function backgroundRefresh() {
  if (backgroundRefreshInFlight === null) {
    backgroundRefreshInFlight = refreshAll()
      .catch(() => {
        if (els.lastRefreshed) {
          els.lastRefreshed.textContent += " · Data may be outdated";
        }
        showBanner("Data may be outdated: the last refresh failed; previous values are kept.", "error");
      })
      .finally(() => {
        backgroundRefreshInFlight = null;
      });
  }
  return backgroundRefreshInFlight;
}

function schedulePoll() {
  refreshTimer = window.setTimeout(async () => {
    if (!document.hidden) {
      await backgroundRefresh();
    }
    schedulePoll();
  }, POLL_DELAY_MS);
}

function startPolling() {
  if (refreshTimer !== null) {
    clearTimeout(refreshTimer);
  }
  schedulePoll();
}

document.addEventListener("visibilitychange", () => {
  if (!document.hidden && refreshTimer !== null) {
    backgroundRefresh();
  }
});

async function initialize() {
  try {
    await refreshAll();
    startPolling();
    log("Console connected to the backend.");
  } catch (error) {
    showBanner(`Initialization failed: ${error.message}. Data may be outdated until a refresh succeeds.`, "error");
    log(`Initialization failed: ${error.message}`);
    startPolling();
  }
}

initialize();
