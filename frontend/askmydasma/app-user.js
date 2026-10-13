const api = {
  status: "/api/status",
  datasets: "/api/datasets",
  datasetTimeRange: "/api/dataset-time-range",
  datasetPreview: "/api/dataset-preview",
  currentCampaign: "/api/current-campaign",
  campaignProgress: "/api/campaign-progress",
  grafanaUrl: "/api/grafana-url",
  launch: "/api/campaigns/launch",
  startProduction: "/api/campaigns/start-production",
  stopAndReset: "/api/campaigns/stop-and-reset",
};

const els = {
  dataset: document.querySelector("#dataset"),
  datasetHelp: document.querySelector("#dataset-help"),
  mode: document.querySelector("#mode"),
  fields: document.querySelector("#fields"),
  fieldsHelp: document.querySelector("#fields-help"),
  modelHstree: document.querySelector("#model-hstree"),
  modelIforest: document.querySelector("#model-iforest"),
  modelKitnet: document.querySelector("#model-kitnet"),
  modelExplainability: document.querySelector("#model-explainability"),
  modelExplainabilityWrapper: document.querySelector("#model-explainability-wrapper"),
  explainabilityNsamples: document.querySelector("#explainability-nsamples"),
  explainabilityModel: document.querySelector("#explainability-model"),
  explainabilityMode: document.querySelector("#explainability-mode"),
  explainabilityModeError: document.querySelector("#explainability-mode-error"),
  explainabilityErrorRow: document.querySelector("#explainability-error-row"),
  explainabilityV2ModeError: document.querySelector("#explainability-v2-mode-error"),
  explainabilityV2ErrorRow: document.querySelector("#explainability-v2-error-row"),
  iforestEstimatorPanel: document.querySelector("#iforest-estimator-panel"),
  iforestParamsEmpty: document.querySelector("#iforest-params-empty"),
  mdragpointEstimatorPanel: document.querySelector("#mdragpoint-estimator-panel"),
  mdragpointParamsEmpty: document.querySelector("#mdragpoint-params-empty"),
  explainabilityDashboardLink: document.querySelector("#explainability-dashboard-link"),
  explainabilityMethod: document.querySelector("#explainability-method"),
  modelExplainabilityV2: document.querySelector("#model-explainability-v2"),
  modelExplainabilityV2Wrapper: document.querySelector("#model-explainability-v2-wrapper"),
  explainabilityV2Nsamples: document.querySelector("#explainability-v2-nsamples"),
  explainabilityV2Model: document.querySelector("#explainability-v2-model"),
  explainabilityV2Mode: document.querySelector("#explainability-v2-mode"),
  explainabilityV2DashboardLink: document.querySelector("#explainability-v2-dashboard-link"),
  explainabilityV2Method: document.querySelector("#explainability-v2-method"),
  repeatabilityHstree: document.querySelector("#repeatability-hstree"),
  repeatabilityHstreeWrapper: document.querySelector("#repeatability-hstree-wrapper"),
  repeatabilityIforest: document.querySelector("#repeatability-iforest"),
  repeatabilityIforestWrapper: document.querySelector("#repeatability-iforest-wrapper"),
  repeatabilityKitnet: document.querySelector("#repeatability-kitnet"),
  repeatabilityKitnetWrapper: document.querySelector("#repeatability-kitnet-wrapper"),
  repeatabilityDeepant: document.querySelector("#repeatability-deepant"),
  repeatabilityDeepantWrapper: document.querySelector("#repeatability-deepant-wrapper"),
  repeatabilityMdragpoint: document.querySelector("#repeatability-mdragpoint"),
  repeatabilityMdragpointWrapper: document.querySelector("#repeatability-mdragpoint-wrapper"),
  reproducibilityHstree: document.querySelector("#reproducibility-hstree"),
  reproducibilityHstreeWrapper: document.querySelector("#reproducibility-hstree-wrapper"),
  reproducibilityIforest: document.querySelector("#reproducibility-iforest"),
  reproducibilityIforestWrapper: document.querySelector("#reproducibility-iforest-wrapper"),
  reproducibilityKitnet: document.querySelector("#reproducibility-kitnet"),
  reproducibilityKitnetWrapper: document.querySelector("#reproducibility-kitnet-wrapper"),
  reproducibilityDeepant: document.querySelector("#reproducibility-deepant"),
  reproducibilityDeepantWrapper: document.querySelector("#reproducibility-deepant-wrapper"),
  reproducibilityMdragpoint: document.querySelector("#reproducibility-mdragpoint"),
  reproducibilityMdragpointWrapper: document.querySelector("#reproducibility-mdragpoint-wrapper"),
  reproducibilitySourceRole: document.querySelector("#reproducibility-source-role"),
  reproducibilityCampaignId: document.querySelector("#reproducibility-campaign-id"),
  reproducibilitySettings: document.querySelector("#reproducibility-settings"),
  validationModeFieldset: document.querySelector("#validation-mode"),
  validationModeRepeatability: document.querySelector("#validation-mode-repeatability"),
  validationModeReproducibility: document.querySelector("#validation-mode-reproducibility"),
  hstreeParams: document.querySelector("#hstree-params"),
  hstreeEstimator: document.querySelector("#hstree-estimator"),
  hstreeP99Params: document.querySelector("#hstree-p99-params"),
  hstreeWelfordParams: document.querySelector("#hstree-welford-params"),
  hstreeWelfordWindowParams: document.querySelector("#hstree-welford-window-params"),
  hstreeWindow: document.querySelector("#hstree-window"),
  hstreeKSigma: document.querySelector("#hstree-k-sigma"),
  hstreeWarmupWindow: document.querySelector("#hstree-warmup-window"),
  hstreeWelfordWindow: document.querySelector("#hstree-welford-window"),
  hstreeInitialWindowSize: document.querySelector("#hstree-initial-window-size"),
  hstreeWindowSize: document.querySelector("#hstree-window-size"),
  hstreeNumTrees: document.querySelector("#hstree-num-trees"),
  hstreeVariant: document.querySelector("#hstree-variant"),
  hstreeMaxDepth: document.querySelector("#hstree-max-depth"),
  iforestParams: document.querySelector("#iforest-params"),
  iforestEstimator: document.querySelector("#iforest-estimator"),
  iforestWelfordParams: document.querySelector("#iforest-welford-params"),
  iforestWelfordWindowParams: document.querySelector("#iforest-welford-window-params"),
  iforestP99Params: document.querySelector("#iforest-p99-params"),
  iforestKSigma: document.querySelector("#iforest-k-sigma"),
  iforestWarmupWindow: document.querySelector("#iforest-warmup-window"),
  iforestWelfordWindow: document.querySelector("#iforest-welford-window"),
  iforestWindow: document.querySelector("#iforest-window"),
  iforestInitialWindowSize: document.querySelector("#iforest-initial-window-size"),
  iforestWindowSize: document.querySelector("#iforest-window-size"),
  iforestNEstimators: document.querySelector("#iforest-n-estimators"),
  iforestMaxSamples: document.querySelector("#iforest-max-samples"),
  iforestRefitEvery: document.querySelector("#iforest-refit-every"),
  iforestContamination: document.querySelector("#iforest-contamination"),
  kitnetParams: document.querySelector("#kitnet-params"),
  kitnetEstimator: document.querySelector("#kitnet-estimator"),
  kitnetWelfordParams: document.querySelector("#kitnet-welford-params"),
  kitnetWelfordWindowParams: document.querySelector("#kitnet-welford-window-params"),
  kitnetP99Params: document.querySelector("#kitnet-p99-params"),
  kitnetKSigma: document.querySelector("#kitnet-k-sigma"),
  kitnetWarmupWindow: document.querySelector("#kitnet-warmup-window"),
  kitnetWelfordWindow: document.querySelector("#kitnet-welford-window"),
  kitnetWindow: document.querySelector("#kitnet-window"),
  kitnetMaxSizeAe: document.querySelector("#kitnet-max-size-ae"),
  kitnetGraceFeatureMapping: document.querySelector("#kitnet-grace-feature-mapping"),
  kitnetGraceAnomalyDetector: document.querySelector("#kitnet-grace-anomaly-detector"),
  kitnetLearningRate: document.querySelector("#kitnet-learning-rate"),
  kitnetHiddenRatio: document.querySelector("#kitnet-hidden-ratio"),
  kitnetClip: document.querySelector("#kitnet-clip"),
  modelDeepant: document.querySelector("#model-deepant"),
  deepantParams: document.querySelector("#deepant-params"),
  deepantEstimator: document.querySelector("#deepant-estimator"),
  deepantP99Params: document.querySelector("#deepant-p99-params"),
  deepantWelfordParams: document.querySelector("#deepant-welford-params"),
  deepantWelfordWindowParams: document.querySelector("#deepant-welford-window-params"),
  deepantWindow: document.querySelector("#deepant-window"),
  deepantPercentile: document.querySelector("#deepant-percentile"),
  deepantKSigma: document.querySelector("#deepant-k-sigma"),
  deepantWarmupWindow: document.querySelector("#deepant-warmup-window"),
  deepantWelfordWindow: document.querySelector("#deepant-welford-window"),
  deepantLookbackSize: document.querySelector("#deepant-lookback-size"),
  deepantInitialWindowSize: document.querySelector("#deepant-initial-window-size"),
  deepantRefitEvery: document.querySelector("#deepant-refit-every"),
  deepantWindowSize: document.querySelector("#deepant-window-size"),
  deepantFilters: document.querySelector("#deepant-filters"),
  deepantKernelSize: document.querySelector("#deepant-kernel-size"),
  deepantDropout: document.querySelector("#deepant-dropout"),
  deepantLr: document.querySelector("#deepant-lr"),
  deepantEpochsInitial: document.querySelector("#deepant-epochs-initial"),
  deepantEpochsRefit: document.querySelector("#deepant-epochs-refit"),
  deepantBatchSize: document.querySelector("#deepant-batch-size"),
  deepantMaxGroupWorkers: document.querySelector("#deepant-max-group-workers"),
  modelMdragpoint: document.querySelector("#model-mdragpoint"),
  mdragpointParams: document.querySelector("#mdragpoint-params"),
  mdragpointEstimatorUnified: document.querySelector("#mdragpoint-estimator-unified"),
  mdragpointVerdictMode: document.querySelector("#mdragpoint-verdict-mode"),
  mdragpointExternalParams: document.querySelector("#mdragpoint-external-params"),
  mdragpointEstimator: document.querySelector("#mdragpoint-estimator"),
  mdragpointP99Params: document.querySelector("#mdragpoint-p99-params"),
  mdragpointWelfordParams: document.querySelector("#mdragpoint-welford-params"),
  mdragpointWelfordWindowParams: document.querySelector("#mdragpoint-welford-window-params"),
  mdragpointWindow: document.querySelector("#mdragpoint-window"),
  mdragpointPercentile: document.querySelector("#mdragpoint-percentile"),
  mdragpointKSigma: document.querySelector("#mdragpoint-k-sigma"),
  mdragpointWarmupWindow: document.querySelector("#mdragpoint-warmup-window"),
  mdragpointWelfordWindow: document.querySelector("#mdragpoint-welford-window"),
  mdragpointDynamicThreshold: document.querySelector("#mdragpoint-dynamic-threshold"),
  mdragpointLofInit: document.querySelector("#mdragpoint-lof-init"),
  mdragpointNInit: document.querySelector("#mdragpoint-n-init"),
  mdragpointMaxNbClusters: document.querySelector("#mdragpoint-max-nb-clusters"),
  mdragpointMaxClusterSize: document.querySelector("#mdragpoint-max-cluster-size"),
  mdragpointLofNeighbors: document.querySelector("#mdragpoint-lof-neighbors"),
  datasetTimeRangeBlock: document.querySelector("#dataset-time-range-block"),
  datasetRowCount: document.querySelector("#dataset-row-count"),
  datasetTimeColumn: document.querySelector("#dataset-time-column"),
  datasetTimeStart: document.querySelector("#dataset-time-start"),
  datasetTimeEnd: document.querySelector("#dataset-time-end"),
  datasetTimeDuration: document.querySelector("#dataset-time-duration"),
  datasetTimeStatus: document.querySelector("#dataset-time-status"),
  datasetPreviewBlock: document.querySelector("#dataset-preview-block"),
  datasetPreviewStatus: document.querySelector("#dataset-preview-status"),
  datasetPreviewLayout: document.querySelector("#dataset-preview-layout"),
  datasetPreviewWindow: document.querySelector("#dataset-preview-window"),
  datasetPreviewTable: document.querySelector("#dataset-preview-table"),
  datasetPreviewHeader: document.querySelector("#dataset-preview-header"),
  datasetPreviewBody: document.querySelector("#dataset-preview-body"),
  datasetPreviewCards: document.querySelector("#dataset-preview-cards"),
  launchButton: document.querySelector("#launch-button"),
  stopResetButton: document.querySelector("#stop-reset-button"),
  platformState: document.querySelector("#platform-state"),
  grafanaLink: document.querySelector("#grafana-link"),
  modelDashboardLinks: document.querySelectorAll("[data-grafana-path]"),
  form: document.querySelector("#launch-form"),
};

const datasetStorageKey = "askmydasma.dataset";

let datasetPresets = {};
let currentDatasetPreview = null;
let currentCampaign = null;
let currentPlatformStatus = null;
let refreshTimer = null;
let actionInFlight = false;
let datasetDirty = false;
let modeDirty = false;
let fieldsDirty = false;
let modelsDirty = false;
let hstreeParamsDirty = false;
let hstreeCoreParamsDirty = false;
let iforestParamsDirty = false;
let iforestCoreParamsDirty = false;
let kitnetParamsDirty = false;
let kitnetCoreParamsDirty = false;
let deepantParamsDirty = false;
let deepantCoreParamsDirty = false;
let mdragpointParamsDirty = false;
let mdragpointCoreParamsDirty = false;
let explainabilityParamsDirty = false;
let repeatabilityDirty = false;
let reproducibilityDirty = false;
let validationModeDirty = false;
let pendingCampaignPayload = null;
let grafanaBaseUrl = "http://localhost:3000";

const explainabilityDashboardPaths = {
  v1: {
    hstree: "/d/explainability-influxdb/explainability?orgId=1&from=now-30m&to=now&timezone=browser&var-group=ALL&refresh=10s",
    iforest: "/d/explainability-iforest-influxdb/explainability-iforest?orgId=1&from=now-30m&to=now&timezone=browser&var-group=ALL&refresh=10s",
    kitnet: "/d/explainability-kitnet-influxdb/explainability-kitnet?orgId=1&from=now-30m&to=now&timezone=browser&var-group=ALL&refresh=10s",
  },
  v2: {
    hstree: "/d/explainability-v2-influxdb/explainability-v2?orgId=1&from=now-30m&to=now&timezone=browser&var-group=ALL&refresh=10s",
    iforest: "/d/explainability-v2-iforest-influxdb/explainability-v2-iforest?orgId=1&from=now-30m&to=now&timezone=browser&var-group=ALL&refresh=10s",
    kitnet: "/d/explainability-v2-kitnet-influxdb/explainability-v2-kitnet?orgId=1&from=now-30m&to=now&timezone=browser&var-group=ALL&refresh=10s",
  },
};

const VALIDATION_MODELS = ["hstree", "iforest", "kitnet", "deepant", "mdragpoint"];

function prettyLabel(value) {
  if (!value) {
    return "-";
  }
  if (value === "realtime") {
    return "Realtime 1 msg/s";
  }
  if (value === "realtime-x5") {
    return "Realtime 5 msg/s";
  }
  if (value === "realtime-x20") {
    return "Realtime 20 msg/s";
  }
  if (value === "realtime-x100") {
    return "Realtime 100 msg/s";
  }
  if (value === "not_configured") {
    return "not configured";
  }
  if (value === "unavailable") {
    return "unavailable";
  }
  if (value === "reseted") {
    return "reset";
  }
  if (value === "resetting") {
    return "resetting";
  }
  if (value === "waiting_for_launch") {
    return "waiting for launch";
  }
  if (value === "preparing_platform") {
    return "preparing the platform";
  }
  if (value === "processing_data") {
    return "platform processing data";
  }
  if (value === "stopping_platform") {
    return "stopping the platform";
  }
  if (value === "resetting_platform") {
    return "resetting the platform";
  }
  if (value === "ready_for_production") {
    return "ready for production";
  }
  if (value === "starting") {
    return "starting";
  }
  return String(value).replaceAll("_", " ");
}

function applyGrafanaLinks(baseUrl) {
  grafanaBaseUrl = (baseUrl || grafanaBaseUrl).replace(/\/+$/, "");
  const normalizedBaseUrl = grafanaBaseUrl;
  for (const link of els.modelDashboardLinks) {
    const path = link.dataset.grafanaPath;
    if (path) {
      link.href = `${normalizedBaseUrl}${path.startsWith("/") ? path : `/${path}`}`;
    }
  }
}

function updateExplainabilityDashboardLinks() {
  for (const [link, select, version] of [
    [els.explainabilityDashboardLink, els.explainabilityModel, "v1"],
    [els.explainabilityV2DashboardLink, els.explainabilityV2Model, "v2"],
  ]) {
    if (!link || !select) continue;
    const model = ["hstree", "iforest", "kitnet"].includes(select.value) ? select.value : "hstree";
    const path = explainabilityDashboardPaths[version][model];
    link.dataset.grafanaPath = path;
    link.href = `${grafanaBaseUrl}${path}`;
  }
}

function setBusyState(isBusy) {
  actionInFlight = isBusy;
  updateInteractiveState();
}

function setPlatformState(status) {
  if (els.platformState) {
    els.platformState.textContent = prettyLabel(status);
  }
}

function isCampaignPrepared(campaign, platformStatus) {
  return campaign?.status === "configuring" || platformStatus === "ready_for_production";
}

function isCampaignRunning(campaign, platformStatus) {
  return campaign?.status === "running" || platformStatus === "processing_data";
}

function repeatabilityBoxes() {
  return [
    { model: "hstree", box: els.repeatabilityHstree, wrapper: els.repeatabilityHstreeWrapper, modelBox: els.modelHstree },
    { model: "iforest", box: els.repeatabilityIforest, wrapper: els.repeatabilityIforestWrapper, modelBox: els.modelIforest },
    { model: "kitnet", box: els.repeatabilityKitnet, wrapper: els.repeatabilityKitnetWrapper, modelBox: els.modelKitnet },
    { model: "deepant", box: els.repeatabilityDeepant, wrapper: els.repeatabilityDeepantWrapper, modelBox: els.modelDeepant },
    { model: "mdragpoint", box: els.repeatabilityMdragpoint, wrapper: els.repeatabilityMdragpointWrapper, modelBox: els.modelMdragpoint },
  ];
}

function enforceSingleRepeatability(sourceModel) {
  // Rule: at most one repeatability checkbox may stay checked. The checkbox the
  // user just ticked (sourceModel) wins; for restored inconsistent campaigns
  // the first listed one wins. Other anomaly models are never deselected here.
  const checked = repeatabilityBoxes().filter((entry) => entry.box?.checked).map((entry) => entry.model);
  let active = null;
  if (sourceModel && checked.includes(sourceModel)) {
    active = sourceModel;
  } else if (checked.length > 0) {
    active = checked[0];
  }
  for (const entry of repeatabilityBoxes()) {
    if (entry.box && entry.model !== active) {
      entry.box.checked = false;
    }
  }
  return active;
}

function modelSelectorBoxes() {
  // Every model selection checkbox, including the explainability services.
  return [
    { model: "hstree", box: els.modelHstree },
    { model: "iforest", box: els.modelIforest },
    { model: "kitnet", box: els.modelKitnet },
    { model: "deepant", box: els.modelDeepant },
    { model: "mdragpoint", box: els.modelMdragpoint },
    { model: "explainability", box: els.modelExplainability },
    { model: "explainability-v2", box: els.modelExplainabilityV2 },
  ];
}

function enforceRepeatabilityExclusivity(sourceModel) {
  // Exclusive dedicated test: when one repeatability is active, the selection
  // is frozen to that model. Every other model selector (including
  // explainability services) and every other repeatability checkbox is
  // unchecked; the flagged model stays checked and its own repeatability
  // checkbox stays usable so the user can leave the mode by unchecking it.
  // Availability (disabled states) is applied by updateInteractiveState.
  // sourceModel (user action) wins over the first listed checkbox for
  // restored inconsistent campaigns (the flagged model wins).
  const active = enforceSingleRepeatability(sourceModel);
  if (active === null) {
    return null;
  }
  for (const entry of repeatabilityBoxes()) {
    if (entry.model === active) {
      if (entry.modelBox) entry.modelBox.checked = true;
    } else {
      if (entry.box) entry.box.checked = false;
      if (entry.modelBox) entry.modelBox.checked = false;
    }
  }
  // A repeatability flag excludes every reproducibility flag.
  for (const entry of reproducibilityBoxes()) {
    if (entry.box) entry.box.checked = false;
  }
  if (els.modelExplainability) els.modelExplainability.checked = false;
  if (els.modelExplainabilityV2) els.modelExplainabilityV2.checked = false;
  return active;
}

function reproducibilityBoxes() {
  return [
    { model: "hstree", box: els.reproducibilityHstree, wrapper: els.reproducibilityHstreeWrapper, modelBox: els.modelHstree },
    { model: "iforest", box: els.reproducibilityIforest, wrapper: els.reproducibilityIforestWrapper, modelBox: els.modelIforest },
    { model: "kitnet", box: els.reproducibilityKitnet, wrapper: els.reproducibilityKitnetWrapper, modelBox: els.modelKitnet },
    { model: "deepant", box: els.reproducibilityDeepant, wrapper: els.reproducibilityDeepantWrapper, modelBox: els.modelDeepant },
    { model: "mdragpoint", box: els.reproducibilityMdragpoint, wrapper: els.reproducibilityMdragpointWrapper, modelBox: els.modelMdragpoint },
  ];
}

function enforceSingleReproducibility(sourceModel) {
  // Rule: at most one Reproductibilité checkbox may stay checked. The checkbox
  // the user just ticked (sourceModel) wins; for restored inconsistent
  // campaigns the first canonical model wins.
  const checked = reproducibilityBoxes().filter((entry) => entry.box?.checked).map((entry) => entry.model);
  let active = null;
  if (sourceModel && checked.includes(sourceModel)) {
    active = sourceModel;
  } else if (checked.length > 0) {
    active = checked[0];
  }
  for (const entry of reproducibilityBoxes()) {
    if (entry.box && entry.model !== active) {
      entry.box.checked = false;
    }
  }
  return active;
}

function enforceReproducibilityExclusivity(sourceModel) {
  // Exclusive dedicated reproducibility test: when one Reproductibilité flag
  // is active, model M is the only selected model, every repeatability flag
  // is unchecked and the role/campaign-id controls become usable. The
  // Reproductibilité checkbox of M stays usable to leave the mode.
  // Availability (disabled states) is applied by updateInteractiveState.
  // sourceModel (user action) wins over the first canonical model for
  // restored inconsistent campaigns.
  const active = enforceSingleReproducibility(sourceModel);
  if (active === null) {
    return null;
  }
  for (const entry of reproducibilityBoxes()) {
    if (entry.model === active) {
      if (entry.modelBox) entry.modelBox.checked = true;
    } else {
      if (entry.box) entry.box.checked = false;
      if (entry.modelBox) entry.modelBox.checked = false;
    }
  }
  // A reproducibility campaign excludes every repeatability flag.
  for (const entry of repeatabilityBoxes()) {
    if (entry.box) entry.box.checked = false;
  }
  for (const entry of modelSelectorBoxes()) {
    if (entry.model !== active && entry.box) {
      entry.box.checked = false;
    }
  }
  return active;
}

function activeValidationMode() {
  // The two global checkboxes are presentation-only controls; the per-model
  // flags remain the source of truth sent to the API.
  if (els.validationModeRepeatability?.checked) {
    return "repeatability";
  }
  if (els.validationModeReproducibility?.checked) {
    return "reproducibility";
  }
  return null;
}

function applyValidationModeBoxes(mode) {
  // Sets the two global checkboxes only. It never clears validation data, so
  // the campaign-sync path can deduce a mode without destroying the flags it
  // just restored.
  if (els.validationModeRepeatability) {
    els.validationModeRepeatability.checked = mode === "repeatability";
  }
  if (els.validationModeReproducibility) {
    els.validationModeReproducibility.checked = mode === "reproducibility";
  }
}

function resetRepeatabilityFlags() {
  for (const entry of repeatabilityBoxes()) {
    if (entry.box) entry.box.checked = false;
  }
}

function resetReproducibilityFields() {
  for (const entry of reproducibilityBoxes()) {
    if (entry.box) entry.box.checked = false;
  }
  if (els.reproducibilitySourceRole) {
    els.reproducibilitySourceRole.value = "";
  }
  if (els.reproducibilityCampaignId) {
    els.reproducibilityCampaignId.value = "";
  }
}

function setValidationMode(mode) {
  // User-action path: enforce the global exclusivity and clear ONLY the
  // opposite mode's data. The active mode's own flags are never cleared here,
  // so deducing a mode during a campaign sync keeps its flags intact.
  applyValidationModeBoxes(mode);
  if (mode === "repeatability") {
    resetReproducibilityFields();
  } else if (mode === "reproducibility") {
    resetRepeatabilityFlags();
  }
}

function explainabilityServicesMissingMode() {
  const missing = [];
  if (els.modelExplainability?.checked && !selectedExplainabilityMode(els.explainabilityMode)) {
    missing.push("Explainability V1");
  }
  if (els.modelExplainabilityV2?.checked && !selectedExplainabilityMode(els.explainabilityV2Mode)) {
    missing.push("Explainability V2");
  }
  return missing;
}

function updateInteractiveState() {
  updateExplainabilityDashboardLinks();
  for (const [model, method, samples] of [
    [els.explainabilityModel, els.explainabilityMethod, els.explainabilityNsamples],
    [els.explainabilityV2Model, els.explainabilityV2Method, els.explainabilityV2Nsamples],
  ]) {
    const computeWarning = model?.closest(".model-explain-field")?.querySelector(".explainability-compute-warning");
    if (computeWarning) computeWarning.hidden = model?.value !== "kitnet";
    const treeOption = method?.querySelector('option[value="tree"]');
    if (treeOption) treeOption.disabled = model?.value !== "iforest";
    if (method?.value === "tree" && model?.value !== "iforest") method.value = "kernel";
    const tree = method?.value === "tree";
    const parametersPanel = samples?.closest(".model-params-panel");
    if (parametersPanel) {
      parametersPanel.classList.toggle("is-hidden", tree);
      parametersPanel.hidden = tree;
      if (tree) parametersPanel.open = false;
      const parametersEmpty = parametersPanel.parentElement?.querySelector(".param-empty");
      parametersEmpty?.classList.toggle("is-hidden", !tree);
    }
  }
  const prepared = isCampaignPrepared(currentCampaign, currentPlatformStatus);
  const running = isCampaignRunning(currentCampaign, currentPlatformStatus);
  const starting = currentCampaign?.status === "starting";
  const launchLabel = starting ? "Starting..." : running ? "Analysis running" : prepared ? "Start production" : "Start analysis";
  const locked = actionInFlight || running;

  // Repeatability UX rules, centralized in the helpers above.
  // Rule 1: a repeatability checkbox only exists for a checked model.
  for (const entry of repeatabilityBoxes()) {
    if (entry.box && !(entry.modelBox?.checked ?? false)) {
      entry.box.checked = false;
    }
  }
  // Global validation mode: per-model validation flags may only exist while
  // their mode is active. With no mode selected, every validation field is
  // reset so an ordinary campaign always publishes neutral values.
  const validationMode = activeValidationMode();
  if (validationMode === null) {
    resetRepeatabilityFlags();
    resetReproducibilityFields();
  }
  // Exclusive dedicated tests: normalize restored inconsistent campaigns. A
  // user action wins via the listeners; without one, an active Reproductibilité
  // wins over a Répétabilité, then the first canonical model wins. The server
  // keeps the final authority.
  const activeReproducibility = enforceReproducibilityExclusivity(null);
  const activeRepeatability = activeReproducibility === null ? enforceRepeatabilityExclusivity(null) : null;
  // Rules 1+2 availability: wrappers follow the global mode and their model;
  // the active test's own checkbox stays usable to leave the mode; model
  // selectors are frozen while a dedicated test is active; role/campaign-id
  // are usable only in reproducibility mode.
  for (const entry of repeatabilityBoxes()) {
    const modelChecked = entry.modelBox?.checked ?? false;
    const wrapperHidden = validationMode !== "repeatability" || !modelChecked;
    if (entry.wrapper) {
      entry.wrapper.hidden = wrapperHidden;
      entry.wrapper.classList.toggle("is-hidden", wrapperHidden);
    }
    if (entry.box) entry.box.disabled = locked || activeReproducibility !== null || (!modelChecked && entry.model !== activeRepeatability);
  }
  for (const entry of reproducibilityBoxes()) {
    const modelChecked = entry.modelBox?.checked ?? false;
    const wrapperHidden = validationMode !== "reproducibility" || !modelChecked;
    if (entry.wrapper) {
      entry.wrapper.hidden = wrapperHidden;
      entry.wrapper.classList.toggle("is-hidden", wrapperHidden);
    }
    if (entry.box) entry.box.disabled = locked || activeRepeatability !== null || (!modelChecked && entry.model !== activeReproducibility);
  }
  for (const entry of modelSelectorBoxes()) {
    if (entry.box) entry.box.disabled = locked || activeRepeatability !== null || activeReproducibility !== null;
  }
  if (els.validationModeRepeatability) {
    els.validationModeRepeatability.disabled = locked;
  }
  if (els.validationModeReproducibility) {
    els.validationModeReproducibility.disabled = locked;
  }
  if (els.reproducibilitySettings) {
    const settingsHidden = validationMode !== "reproducibility";
    els.reproducibilitySettings.hidden = settingsHidden;
    els.reproducibilitySettings.classList.toggle("is-hidden", settingsHidden);
  }
  if (els.reproducibilitySourceRole) {
    els.reproducibilitySourceRole.disabled = locked || activeReproducibility === null;
  }
  if (els.reproducibilityCampaignId) {
    els.reproducibilityCampaignId.disabled = locked || activeReproducibility === null;
  }

  const missingExplainabilityModes = explainabilityServicesMissingMode();
  const missingV1 = missingExplainabilityModes.includes("Explainability V1");
  const missingV2 = missingExplainabilityModes.includes("Explainability V2");
  if (els.explainabilityModeError) {
    els.explainabilityModeError.textContent = missingV1
      ? "Choose a mode for each selected explainability service: Explainability V1."
      : "";
    els.explainabilityErrorRow.hidden = !missingV1;
  }
  if (els.explainabilityV2ModeError) {
    els.explainabilityV2ModeError.textContent = missingV2
      ? "Choose a mode for each selected explainability service: Explainability V2."
      : "";
    els.explainabilityV2ErrorRow.hidden = !missingV2;
  }
  if (els.launchButton) {
    els.launchButton.textContent = launchLabel;
    els.launchButton.disabled = actionInFlight || running || missingExplainabilityModes.length > 0;
  }
  for (const element of [els.dataset, els.mode, els.fields]) {
    element.disabled = locked;
  }
  for (const cb of [els.modelHstree, els.modelIforest, els.modelKitnet]) {
    if (cb) cb.disabled = locked;
  }
  if (els.modelExplainability) {
    const explainabilityAvailable = !locked && !els.repeatabilityHstree?.checked && activeReproducibility === null;
    els.modelExplainability.disabled = !explainabilityAvailable;
    if (els.modelExplainabilityWrapper) {
      els.modelExplainabilityWrapper.classList.toggle("is-disabled", !explainabilityAvailable);
    }
  }
  if (els.modelExplainabilityV2) {
    const explainabilityAvailable = !locked && !els.repeatabilityHstree?.checked && activeReproducibility === null;
    els.modelExplainabilityV2.disabled = !explainabilityAvailable;
    if (els.modelExplainabilityV2Wrapper) {
      els.modelExplainabilityV2Wrapper.classList.toggle("is-disabled", !explainabilityAvailable);
    }
  }
  if (els.hstreeParams) {
    const hstreeChecked = (els.modelHstree?.checked ?? false) || els.explainabilityModel?.value === "hstree" || els.explainabilityV2Model?.value === "hstree";
    const hstreeMode = els.hstreeEstimator?.value ?? "p99";
    els.hstreeParams.classList.toggle("is-disabled", locked || !hstreeChecked);
    if (els.hstreeEstimator) els.hstreeEstimator.disabled = locked || !hstreeChecked;
    if (els.hstreeP99Params) els.hstreeP99Params.classList.toggle("is-hidden", hstreeMode !== "p99");
    if (els.hstreeWelfordParams) els.hstreeWelfordParams.classList.toggle("is-hidden", hstreeMode !== "welford" && hstreeMode !== "welford-windowed");
    if (els.hstreeWelfordWindowParams) els.hstreeWelfordWindowParams.classList.toggle("is-hidden", hstreeMode !== "welford-windowed");
    if (els.hstreeWindow) els.hstreeWindow.disabled = locked || !hstreeChecked;
    if (els.hstreeKSigma) els.hstreeKSigma.disabled = locked || !hstreeChecked;
    if (els.hstreeWarmupWindow) els.hstreeWarmupWindow.disabled = locked || !hstreeChecked;
    if (els.hstreeWelfordWindow) els.hstreeWelfordWindow.disabled = locked || !hstreeChecked;
  }
  if (els.iforestParams) {
    const iforestChecked = (els.modelIforest?.checked ?? false)
      || (els.modelExplainability?.checked && els.explainabilityModel?.value === "iforest")
      || (els.modelExplainabilityV2?.checked && els.explainabilityV2Model?.value === "iforest");
    const iforestMode = els.iforestEstimator?.value ?? "native";
    els.iforestParams.classList.toggle("is-disabled", locked || !iforestChecked);
    if (els.iforestEstimator) els.iforestEstimator.disabled = locked || !iforestChecked;
    if (els.iforestContamination) els.iforestContamination.disabled = locked || !iforestChecked || iforestMode !== "native";
    if (els.iforestWelfordParams) els.iforestWelfordParams.classList.toggle("is-hidden", iforestMode !== "welford" && iforestMode !== "welford-windowed");
    if (els.iforestWelfordWindowParams) els.iforestWelfordWindowParams.classList.toggle("is-hidden", iforestMode !== "welford-windowed");
    if (els.iforestP99Params) els.iforestP99Params.classList.toggle("is-hidden", iforestMode !== "p99");
    if (els.iforestEstimatorPanel) {
      const panelHidden = iforestMode === "native";
      els.iforestEstimatorPanel.hidden = panelHidden;
      if (panelHidden) els.iforestEstimatorPanel.open = false;
    }
    if (els.iforestParamsEmpty) els.iforestParamsEmpty.classList.toggle("is-hidden", iforestMode !== "native");
    for (const input of [els.iforestKSigma, els.iforestWarmupWindow, els.iforestWindow, els.iforestWelfordWindow]) {
      if (input) input.disabled = locked || !iforestChecked;
    }
  }
  if (els.kitnetParams) {
    const kitnetChecked = (els.modelKitnet?.checked ?? false)
      || (els.modelExplainability?.checked && els.explainabilityModel?.value === "kitnet")
      || (els.modelExplainabilityV2?.checked && els.explainabilityV2Model?.value === "kitnet");
    const kitnetMode = els.kitnetEstimator?.value ?? "welford-windowed";
    els.kitnetParams.classList.toggle("is-disabled", locked || !kitnetChecked);
    if (els.kitnetEstimator) els.kitnetEstimator.disabled = locked || !kitnetChecked;
    if (els.kitnetWelfordParams) els.kitnetWelfordParams.classList.toggle("is-hidden", kitnetMode !== "welford" && kitnetMode !== "welford-windowed");
    if (els.kitnetWelfordWindowParams) els.kitnetWelfordWindowParams.classList.toggle("is-hidden", kitnetMode !== "welford-windowed");
    if (els.kitnetP99Params) els.kitnetP99Params.classList.toggle("is-hidden", kitnetMode !== "p99");
    for (const input of [els.kitnetKSigma, els.kitnetWarmupWindow, els.kitnetWindow, els.kitnetWelfordWindow]) {
      if (input) input.disabled = locked || !kitnetChecked;
    }
  }
  if (els.deepantParams) {
    const deepantChecked = els.modelDeepant?.checked ?? false;
    const deepantMode = els.deepantEstimator?.value ?? "welford-windowed";
    els.deepantParams.classList.toggle("is-disabled", locked || !deepantChecked);
    if (els.deepantEstimator) els.deepantEstimator.disabled = locked || !deepantChecked;
    if (els.deepantP99Params) els.deepantP99Params.classList.toggle("is-hidden", deepantMode !== "p99");
    if (els.deepantWelfordParams) els.deepantWelfordParams.classList.toggle("is-hidden", deepantMode !== "welford" && deepantMode !== "welford-windowed");
    if (els.deepantWelfordWindowParams) els.deepantWelfordWindowParams.classList.toggle("is-hidden", deepantMode !== "welford-windowed");
    for (const input of [
      els.deepantWindow, els.deepantPercentile, els.deepantKSigma, els.deepantWarmupWindow,
      els.deepantWelfordWindow,
      els.deepantLookbackSize, els.deepantInitialWindowSize, els.deepantRefitEvery,
      els.deepantWindowSize, els.deepantFilters, els.deepantKernelSize, els.deepantDropout,
      els.deepantLr, els.deepantEpochsInitial, els.deepantEpochsRefit, els.deepantBatchSize,
      els.deepantMaxGroupWorkers,
    ]) {
      if (input) input.disabled = locked || !deepantChecked;
    }
  }
  if (els.mdragpointParams) {
    const mdragpointChecked = els.modelMdragpoint?.checked ?? false;
    const mdragpointVerdictMode = els.mdragpointVerdictMode?.value ?? "internal";
    const mdragpointMode = els.mdragpointEstimator?.value ?? "p99";
    els.mdragpointParams.classList.toggle("is-disabled", locked || !mdragpointChecked);
    if (els.mdragpointEstimatorUnified) els.mdragpointEstimatorUnified.disabled = locked || !mdragpointChecked;
    if (els.mdragpointExternalParams) els.mdragpointExternalParams.classList.toggle("is-hidden", mdragpointVerdictMode !== "external");
    if (els.mdragpointP99Params) els.mdragpointP99Params.classList.toggle("is-hidden", mdragpointMode !== "p99");
    if (els.mdragpointWelfordParams) els.mdragpointWelfordParams.classList.toggle("is-hidden", mdragpointMode !== "welford" && mdragpointMode !== "welford-windowed");
    if (els.mdragpointWelfordWindowParams) els.mdragpointWelfordWindowParams.classList.toggle("is-hidden", mdragpointMode !== "welford-windowed");
    if (els.mdragpointEstimatorPanel) {
      const panelHidden = mdragpointVerdictMode !== "external";
      els.mdragpointEstimatorPanel.hidden = panelHidden;
      if (panelHidden) els.mdragpointEstimatorPanel.open = false;
    }
    if (els.mdragpointParamsEmpty) els.mdragpointParamsEmpty.classList.toggle("is-hidden", mdragpointVerdictMode === "external");
    for (const input of [els.mdragpointWindow, els.mdragpointPercentile, els.mdragpointKSigma, els.mdragpointWarmupWindow, els.mdragpointWelfordWindow]) {
      if (input) input.disabled = locked || !mdragpointChecked || mdragpointVerdictMode !== "external";
    }
    for (const input of [
      els.mdragpointDynamicThreshold, els.mdragpointLofInit, els.mdragpointNInit,
      els.mdragpointMaxNbClusters, els.mdragpointMaxClusterSize, els.mdragpointLofNeighbors,
    ]) {
      if (input) input.disabled = locked || !mdragpointChecked;
    }
  }
  if (els.stopResetButton) {
    els.stopResetButton.disabled = actionInFlight;
  }
}

function splitCsvLine(value) {
  return String(value || "")
    .split(",")
    .map((item) => item.trim())
    .filter(Boolean);
}

function escapeHtml(value) {
  return String(value)
    .replaceAll("&", "&amp;")
    .replaceAll("<", "&lt;")
    .replaceAll(">", "&gt;")
    .replaceAll('"', "&quot;")
    .replaceAll("'", "&#39;");
}

function formatDuration(seconds) {
  if (seconds === null || seconds === undefined || Number.isNaN(seconds)) {
    return "-";
  }
  const total = Math.max(0, Number(seconds));
  const hours = Math.floor(total / 3600);
  const minutes = Math.floor((total % 3600) / 60);
  const remainingSeconds = Math.floor(total % 60);
  return `${hours}h ${minutes}m ${remainingSeconds}s`;
}

function getStoredDataset() {
  try {
    return window.localStorage.getItem(datasetStorageKey) || "";
  } catch (_) {
    return "";
  }
}

function storeDataset(datasetPath) {
  try {
    if (datasetPath) {
      window.localStorage.setItem(datasetStorageKey, datasetPath);
    }
  } catch (_) {
    // Ignore storage errors.
  }
}

function previewBodyContainer() {
  if (els.datasetPreviewBody) {
    return els.datasetPreviewBody;
  }
  return null;
}

function setPreviewBodyHtml(html) {
  const container = previewBodyContainer();
  if (!container) {
    return;
  }
  container.innerHTML = html;
}

function updatePreviewVerticalSlider() {
  const windowElement = els.datasetPreviewWindow;
  const layout = els.datasetPreviewLayout;
  if (!windowElement || !layout) {
    return;
  }

  const maxScroll = Math.max(0, windowElement.scrollHeight - windowElement.clientHeight);
  const hasOverflow = maxScroll > 0;
  layout.classList.toggle("no-slider", !hasOverflow);
  if (!hasOverflow) {
    windowElement.scrollTop = 0;
  }
}

function renderDatasetPreview(preview) {
  currentDatasetPreview = preview;

  if (!preview || !preview.columns?.length) {
    els.datasetPreviewBlock.classList.remove("is-hidden");
    els.datasetPreviewStatus.textContent = "No preview available for this dataset.";
    els.datasetPreviewHeader.innerHTML = "";
    setPreviewBodyHtml("<tr><td>-</td></tr>");
    if (els.datasetPreviewCards) {
      els.datasetPreviewCards.innerHTML = "";
      els.datasetPreviewCards.classList.add("is-hidden");
    }
    if (els.datasetPreviewTable) {
      els.datasetPreviewTable.classList.remove("is-hidden");
    }
    window.requestAnimationFrame(updatePreviewVerticalSlider);
    return;
  }

  const excludedFields = splitCsvLine(els.fields.value);
  const visibleColumns = preview.columns || [];
  const rows = Array.isArray(preview.sample_rows)
    ? preview.sample_rows
    : preview.sample_row && typeof preview.sample_row === "object"
      ? [preview.sample_row]
      : [];

  els.datasetPreviewBlock.classList.remove("is-hidden");
  if (els.datasetPreviewCards) {
    els.datasetPreviewCards.innerHTML = "";
    els.datasetPreviewCards.classList.add("is-hidden");
  }
  if (els.datasetPreviewTable) {
    els.datasetPreviewTable.classList.remove("is-hidden");
  }

  const headerCells = ['<th class="row-index">#</th>'];
  for (const column of visibleColumns) {
    const excluded = excludedFields.some((value) => value.toLowerCase() === column.toLowerCase());
    headerCells.push(`<th class="${excluded ? "excluded" : ""}">${escapeHtml(column)}</th>`);
  }

  const bodyRows = rows
    .map((row, index) => {
      const rowCells = visibleColumns
        .map((column) => {
          const excluded = excludedFields.some((value) => value.toLowerCase() === column.toLowerCase());
          const rawValue = row?.[column];
          const displayValue = rawValue === null || rawValue === undefined || rawValue === "" ? "-" : String(rawValue);
          return `<td class="${excluded ? "excluded" : ""}">${escapeHtml(displayValue)}</td>`;
        })
        .join("");
      return `<tr><td class="row-index">${index + 1}</td>${rowCells}</tr>`;
    })
    .join("");

  els.datasetPreviewHeader.innerHTML = headerCells.join("");
  setPreviewBodyHtml(bodyRows || "<tr><td>-</td></tr>");
  els.datasetPreviewStatus.textContent = `Showing ${rows.length} row(s), up to first ${preview.max_rows || 10}. Excluded fields are greyed.`;
  window.requestAnimationFrame(updatePreviewVerticalSlider);
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
      // Keep the HTTP summary.
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

function applyDatasetPreset(datasetPath) {
  const preset = datasetPresets[datasetPath];
  if (!preset) {
    els.datasetHelp.innerHTML = "Free dataset. No preset from the reference <code>docker-compose</code>.";
    els.datasetHelp.className = "field-help is-neutral";
    els.fields.value = "";
    els.fieldsHelp.textContent = "Comma-separated list.";
    fieldsDirty = false;
    return;
  }

  const presetLabel = preset.label || "Preset";
  els.datasetHelp.innerHTML = `${presetLabel}. Prefill applied from <code>${preset.source}</code>.`;
  els.datasetHelp.className = "field-help is-reference";
  els.fields.value = preset.excluded_fields.join(", ");
  els.fieldsHelp.textContent = "Prefilled from the reference configuration.";
  fieldsDirty = false;
}

async function refreshDatasetTimeRange(datasetPath, mode) {
  const updateTimeRangeView = (prefix, timeRange) => {
    els[`${prefix}RowCount`].textContent = String(timeRange.row_count ?? "-");
    els[`${prefix}TimeColumn`].textContent = timeRange.time_column || "-";
    els[`${prefix}TimeStart`].textContent = timeRange.start || "-";
    els[`${prefix}TimeEnd`].textContent = timeRange.end || "-";
    els[`${prefix}TimeDuration`].textContent = formatDuration(timeRange.duration_seconds);
    els[`${prefix}TimeStatus`].textContent = prettyLabel(timeRange.status);
  };

  if (mode !== "batch" || !datasetPath) {
    els.datasetTimeRangeBlock.classList.add("is-hidden");
    return;
  }

  try {
    const timeRange = await fetchJson(`${api.datasetTimeRange}?dataset_path=${encodeURIComponent(datasetPath)}`);
    els.datasetTimeRangeBlock.classList.remove("is-hidden");
    updateTimeRangeView("dataset", timeRange);
  } catch (_) {
    els.datasetTimeRangeBlock.classList.remove("is-hidden");
    updateTimeRangeView("dataset", { status: "unavailable" });
  }
}

async function refreshDatasetPreview(datasetPath) {
  if (!datasetPath) {
    currentDatasetPreview = null;
    els.datasetPreviewBlock.classList.add("is-hidden");
    return;
  }

  try {
    const preview = await fetchJson(`${api.datasetPreview}?dataset_path=${encodeURIComponent(datasetPath)}`);
    renderDatasetPreview(preview);
  } catch (_) {
    currentDatasetPreview = null;
    els.datasetPreviewBlock.classList.remove("is-hidden");
    els.datasetPreviewStatus.textContent = "Preview unavailable.";
    els.datasetPreviewHeader.innerHTML = "";
    setPreviewBodyHtml("<tr><td>-</td></tr>");
    if (els.datasetPreviewCards) {
      els.datasetPreviewCards.innerHTML = "";
      els.datasetPreviewCards.classList.add("is-hidden");
    }
    if (els.datasetPreviewTable) {
      els.datasetPreviewTable.classList.remove("is-hidden");
    }
    window.requestAnimationFrame(updatePreviewVerticalSlider);
  }
}

function selectedModels() {
  const models = [];
  if (els.modelHstree?.checked) models.push("hstree");
  if (els.modelIforest?.checked) models.push("iforest");
  if (els.modelKitnet?.checked) models.push("kitnet");
  if (els.modelDeepant?.checked) models.push("deepant");
  if (els.modelMdragpoint?.checked) models.push("mdragpoint");
  if (els.modelExplainability?.checked) models.push("explainability");
  if (els.modelExplainabilityV2?.checked) models.push("explainability-v2");
  return models;
}

function parseFloatParam(value, fallback, minValue) {
  const parsed = parseFloat(value);
  return Number.isFinite(parsed) && parsed >= minValue ? parsed : fallback;
}

function parseIntParam(value, fallback, minValue) {
  const parsed = parseInt(value, 10);
  return Number.isFinite(parsed) && parsed >= minValue ? parsed : fallback;
}

function selectedExplainabilityMode(inputs) {
  return inputs?.value || null;
}

function selectedExplainabilityModel(input) {
  return ["hstree", "iforest"].includes(input?.value) ? input.value : null;
}

function restoreExplainabilityModel(input, value) {
  if (input) input.value = ["hstree", "iforest"].includes(value) ? value : "";
}

function restoreExplainabilityMode(inputs, value) {
  if (inputs) inputs.value = ["simple", "reference"].includes(value) ? value : "";
}

function currentFormPayload() {
  storeDataset(els.dataset.value);
  const validationMode = activeValidationMode();
  return {
    dataset_path: els.dataset.value,
    mode: els.mode.value,
    selected_groups: [],
    excluded_fields: splitCsvLine(els.fields.value),
    selected_models: selectedModels(),
    hstree_estimator: els.hstreeEstimator?.value || "p99",
    hstree_threshold_window: parseIntParam(els.hstreeWindow?.value || "500", 500, 50),
    hstree_welford_window: parseIntParam(els.hstreeWelfordWindow?.value || "2048", 2048, 1),
    hstree_anomaly_k_sigma: parseFloatParam(els.hstreeKSigma?.value || "3.0", 3.0, 0.1),
    hstree_anomaly_warmup_window: parseIntParam(els.hstreeWarmupWindow?.value || "100", 100, 2),
    hstree_initial_window_size: parseIntParam(els.hstreeInitialWindowSize?.value || "256", 256, 1),
    hstree_window_size: parseIntParam(els.hstreeWindowSize?.value || "256", 256, 64),
    hstree_num_trees: parseIntParam(els.hstreeNumTrees?.value || "25", 25, 1),
    hstree_variant: els.hstreeVariant?.value || "paper",
    hstree_max_depth: parseIntParam(els.hstreeMaxDepth?.value || "15", 15, 1),
    iforest_estimator: els.iforestEstimator?.value || "native",
    iforest_threshold_window: parseIntParam(els.iforestWindow?.value || "500", 500, 50),
    iforest_welford_window: parseIntParam(els.iforestWelfordWindow?.value || "2048", 2048, 1),
    iforest_anomaly_k_sigma: parseFloatParam(els.iforestKSigma?.value || "3.0", 3.0, 0.1),
    iforest_anomaly_warmup_window: parseIntParam(els.iforestWarmupWindow?.value || "100", 100, 2),
    iforest_initial_window_size: parseIntParam(els.iforestInitialWindowSize?.value || "512", 512, 1),
    iforest_window_size: parseIntParam(els.iforestWindowSize?.value || "2048", 2048, 64),
    iforest_n_estimators: parseIntParam(els.iforestNEstimators?.value || "100", 100, 1),
    iforest_max_samples: els.iforestMaxSamples?.value || "256",
    iforest_refit_every: parseIntParam(els.iforestRefitEvery?.value || "128", 128, 1),
    iforest_contamination: els.iforestContamination?.value || "auto",
    kitnet_estimator: els.kitnetEstimator?.value || "welford-windowed",
    kitnet_threshold_window: parseIntParam(els.kitnetWindow?.value || "1000", 1000, 50),
    kitnet_welford_window: parseIntParam(els.kitnetWelfordWindow?.value || "1000", 1000, 1),
    kitnet_anomaly_k_sigma: parseFloatParam(els.kitnetKSigma?.value || "3.0", 3.0, 0.1),
    kitnet_anomaly_warmup_window: parseIntParam(els.kitnetWarmupWindow?.value || "100", 100, 2),
    kitnet_max_size_ae: parseIntParam(els.kitnetMaxSizeAe?.value || "10", 10, 1),
    kitnet_grace_feature_mapping: parseIntParam(els.kitnetGraceFeatureMapping?.value || "1000", 1000, 0),
    kitnet_grace_anomaly_detector: parseIntParam(els.kitnetGraceAnomalyDetector?.value || "1000", 1000, 0),
    kitnet_learning_rate: parseFloatParam(els.kitnetLearningRate?.value || "0.1", 0.1, 0.001),
    kitnet_hidden_ratio: parseFloatParam(els.kitnetHiddenRatio?.value || "0.75", 0.75, 0.01),
    kitnet_clip: parseFloatParam(els.kitnetClip?.value || "10.0", 10.0, 0.1),
    deepant_estimator: els.deepantEstimator?.value || "welford-windowed",
    deepant_threshold_window: parseIntParam(els.deepantWindow?.value || "500", 500, 50),
    deepant_welford_window: parseIntParam(els.deepantWelfordWindow?.value || "2048", 2048, 1),
    deepant_threshold_percentile: parseFloatParam(els.deepantPercentile?.value || "99.0", 99.0, 0.1),
    deepant_anomaly_k_sigma: parseFloatParam(els.deepantKSigma?.value || "3.0", 3.0, 0.1),
    deepant_anomaly_warmup_window: parseIntParam(els.deepantWarmupWindow?.value || "100", 100, 2),
    deepant_lookback_size: parseIntParam(els.deepantLookbackSize?.value || "100", 100, 8),
    deepant_initial_window_size: parseIntParam(els.deepantInitialWindowSize?.value || "512", 512, 8),
    deepant_refit_every: parseIntParam(els.deepantRefitEvery?.value || "128", 128, 1),
    deepant_window_size: parseIntParam(els.deepantWindowSize?.value || "2048", 2048, 64),
    deepant_filters: parseIntParam(els.deepantFilters?.value || "32", 32, 1),
    deepant_kernel_size: parseIntParam(els.deepantKernelSize?.value || "2", 2, 1),
    deepant_dropout: parseFloatParam(els.deepantDropout?.value || "0.25", 0.25, 0),
    deepant_lr: parseFloatParam(els.deepantLr?.value || "0.001", 0.001, 0.00001),
    deepant_epochs_initial: parseIntParam(els.deepantEpochsInitial?.value || "10", 10, 1),
    deepant_epochs_refit: parseIntParam(els.deepantEpochsRefit?.value || "5", 5, 1),
    deepant_batch_size: parseIntParam(els.deepantBatchSize?.value || "32", 32, 1),
    deepant_max_group_workers: parseIntParam(els.deepantMaxGroupWorkers?.value || "2", 2, 1),
    mdragpoint_verdict_mode: els.mdragpointVerdictMode?.value || "internal",
    mdragpoint_estimator: els.mdragpointEstimator?.value || "p99",
    mdragpoint_threshold_window: parseIntParam(els.mdragpointWindow?.value || "500", 500, 50),
    mdragpoint_welford_window: parseIntParam(els.mdragpointWelfordWindow?.value || "900", 900, 1),
    mdragpoint_threshold_percentile: parseFloatParam(els.mdragpointPercentile?.value || "99.0", 99.0, 0.1),
    mdragpoint_anomaly_k_sigma: parseFloatParam(els.mdragpointKSigma?.value || "3.0", 3.0, 0.1),
    mdragpoint_anomaly_warmup_window: parseIntParam(els.mdragpointWarmupWindow?.value || "100", 100, 2),
    mdragpoint_dynamic_threshold: els.mdragpointDynamicThreshold?.checked ?? true,
    mdragpoint_lof_init: els.mdragpointLofInit?.checked ?? true,
    mdragpoint_n_init: parseIntParam(els.mdragpointNInit?.value || "1000", 1000, 2),
    mdragpoint_max_nb_clusters: parseIntParam(els.mdragpointMaxNbClusters?.value || "50", 50, 1),
    mdragpoint_max_cluster_size: parseIntParam(els.mdragpointMaxClusterSize?.value || "30", 30, 1),
    mdragpoint_lof_neighbors: parseIntParam(els.mdragpointLofNeighbors?.value || "20", 20, 1),
    hstree_repeatability: validationMode === "repeatability" && (els.repeatabilityHstree?.checked ?? false),
    iforest_repeatability: validationMode === "repeatability" && (els.repeatabilityIforest?.checked ?? false),
    kitnet_repeatability: validationMode === "repeatability" && (els.repeatabilityKitnet?.checked ?? false),
    deepant_repeatability: validationMode === "repeatability" && (els.repeatabilityDeepant?.checked ?? false),
    mdragpoint_repeatability: validationMode === "repeatability" && (els.repeatabilityMdragpoint?.checked ?? false),
    hstree_reproducibility: validationMode === "reproducibility" && (els.reproducibilityHstree?.checked ?? false),
    iforest_reproducibility: validationMode === "reproducibility" && (els.reproducibilityIforest?.checked ?? false),
    kitnet_reproducibility: validationMode === "reproducibility" && (els.reproducibilityKitnet?.checked ?? false),
    deepant_reproducibility: validationMode === "reproducibility" && (els.reproducibilityDeepant?.checked ?? false),
    mdragpoint_reproducibility: validationMode === "reproducibility" && (els.reproducibilityMdragpoint?.checked ?? false),
    reproducibility_source_role: validationMode === "reproducibility" ? (els.reproducibilitySourceRole?.value || "") : "",
    reproducibility_campaign_id: validationMode === "reproducibility" ? (els.reproducibilityCampaignId?.value.trim() || "") : "",
    explainability_nsamples: parseIntParam(els.explainabilityNsamples?.value || "100", 100, 100),
    explainability_v2_nsamples: parseIntParam(els.explainabilityV2Nsamples?.value || "100", 100, 100),
    explainability_model: selectedExplainabilityModel(els.explainabilityModel),
    explainability_mode: selectedExplainabilityMode(els.explainabilityMode),
    explainability_method: els.explainabilityMethod?.value || null,
    explainability_v2_model: selectedExplainabilityModel(els.explainabilityV2Model),
    explainability_v2_mode: selectedExplainabilityMode(els.explainabilityV2Mode),
    explainability_v2_method: els.explainabilityV2Method?.value || null,
  };
}

function syncFormFromCampaign(campaign) {
  if (!campaign) {
    return;
  }

  if (campaign.dataset_path && !datasetDirty && !els.dataset.value) {
    els.dataset.value = campaign.dataset_path;
  }
  const selectedMatchesCampaign = !campaign.dataset_path || els.dataset.value === campaign.dataset_path;

  const campaignActive = ["configuring", "starting", "running"].includes(campaign.status);
  if (campaign.mode && !modeDirty && selectedMatchesCampaign && campaignActive) {
    els.mode.value = campaign.mode;
  }
  if (!fieldsDirty && selectedMatchesCampaign) {
    els.fields.value = campaign.excluded_fields?.join(", ") || "";
    fieldsDirty = false;
  }
  if (campaign.selected_models && !modelsDirty && selectedMatchesCampaign) {
    const models = campaign.selected_models;
    if (els.modelHstree) els.modelHstree.checked = models.includes("hstree");
    if (els.modelIforest) els.modelIforest.checked = models.includes("iforest");
    if (els.modelKitnet) els.modelKitnet.checked = models.includes("kitnet");
    if (els.modelDeepant) els.modelDeepant.checked = models.includes("deepant");
    if (els.modelMdragpoint) els.modelMdragpoint.checked = models.includes("mdragpoint");
    if (els.modelExplainability) els.modelExplainability.checked = models.includes("explainability");
    if (els.modelExplainabilityV2) els.modelExplainabilityV2.checked = models.includes("explainability-v2");
  }
  if (!hstreeParamsDirty && selectedMatchesCampaign) {
    if (campaign.hstree_estimator && els.hstreeEstimator) {
      els.hstreeEstimator.value = campaign.hstree_estimator;
    }
    if (campaign.hstree_threshold_window && els.hstreeWindow) {
      els.hstreeWindow.value = String(campaign.hstree_threshold_window);
    }
    if (campaign.hstree_anomaly_k_sigma !== undefined && els.hstreeKSigma) {
      els.hstreeKSigma.value = String(campaign.hstree_anomaly_k_sigma);
    }
    if (campaign.hstree_anomaly_warmup_window !== undefined && els.hstreeWarmupWindow) {
      els.hstreeWarmupWindow.value = String(campaign.hstree_anomaly_warmup_window);
    }
    if (campaign.hstree_welford_window !== undefined && els.hstreeWelfordWindow) {
      els.hstreeWelfordWindow.value = String(campaign.hstree_welford_window);
    }
  }
  if (!hstreeCoreParamsDirty && selectedMatchesCampaign) {
    if (campaign.hstree_initial_window_size !== undefined && els.hstreeInitialWindowSize) {
      els.hstreeInitialWindowSize.value = String(campaign.hstree_initial_window_size);
    }
    if (campaign.hstree_window_size !== undefined && els.hstreeWindowSize) {
      els.hstreeWindowSize.value = String(campaign.hstree_window_size);
    }
    if (campaign.hstree_num_trees !== undefined && els.hstreeNumTrees) {
      els.hstreeNumTrees.value = String(campaign.hstree_num_trees);
    }
    if (campaign.hstree_variant && els.hstreeVariant) {
      els.hstreeVariant.value = campaign.hstree_variant;
    }
    if (campaign.hstree_max_depth !== undefined && els.hstreeMaxDepth) {
      els.hstreeMaxDepth.value = String(campaign.hstree_max_depth);
    }
  }
  if (!iforestParamsDirty && selectedMatchesCampaign) {
    if (campaign.iforest_estimator && els.iforestEstimator) {
      els.iforestEstimator.value = campaign.iforest_estimator;
    }
    if (campaign.iforest_threshold_window && els.iforestWindow) {
      els.iforestWindow.value = String(campaign.iforest_threshold_window);
    }
    if (campaign.iforest_anomaly_k_sigma !== undefined && els.iforestKSigma) {
      els.iforestKSigma.value = String(campaign.iforest_anomaly_k_sigma);
    }
    if (campaign.iforest_anomaly_warmup_window !== undefined && els.iforestWarmupWindow) {
      els.iforestWarmupWindow.value = String(campaign.iforest_anomaly_warmup_window);
    }
    if (campaign.iforest_welford_window !== undefined && els.iforestWelfordWindow) {
      els.iforestWelfordWindow.value = String(campaign.iforest_welford_window);
    }
  }
  if (!iforestCoreParamsDirty && selectedMatchesCampaign) {
    if (els.iforestContamination) {
      const contamination = campaign.iforest_contamination ?? "auto";
      els.iforestContamination.value = contamination === "auto" ? "auto" : Number(contamination).toFixed(2);
    }
    if (campaign.iforest_initial_window_size !== undefined && els.iforestInitialWindowSize) {
      els.iforestInitialWindowSize.value = String(campaign.iforest_initial_window_size);
    }
    if (campaign.iforest_window_size !== undefined && els.iforestWindowSize) {
      els.iforestWindowSize.value = String(campaign.iforest_window_size);
    }
    if (campaign.iforest_n_estimators !== undefined && els.iforestNEstimators) {
      els.iforestNEstimators.value = String(campaign.iforest_n_estimators);
    }
    if (campaign.iforest_max_samples !== undefined && els.iforestMaxSamples) {
      els.iforestMaxSamples.value = campaign.iforest_max_samples;
    }
    if (campaign.iforest_refit_every !== undefined && els.iforestRefitEvery) {
      els.iforestRefitEvery.value = String(campaign.iforest_refit_every);
    }
  }
  if (!kitnetParamsDirty && selectedMatchesCampaign) {
    if (campaign.kitnet_estimator && els.kitnetEstimator) {
      els.kitnetEstimator.value = campaign.kitnet_estimator;
    }
    if (campaign.kitnet_threshold_window && els.kitnetWindow) {
      els.kitnetWindow.value = String(campaign.kitnet_threshold_window);
    }
    if (campaign.kitnet_anomaly_k_sigma !== undefined && els.kitnetKSigma) {
      els.kitnetKSigma.value = String(campaign.kitnet_anomaly_k_sigma);
    }
    if (campaign.kitnet_anomaly_warmup_window !== undefined && els.kitnetWarmupWindow) {
      els.kitnetWarmupWindow.value = String(campaign.kitnet_anomaly_warmup_window);
    }
    if (campaign.kitnet_welford_window !== undefined && els.kitnetWelfordWindow) {
      els.kitnetWelfordWindow.value = String(campaign.kitnet_welford_window);
    }
  }
  if (!kitnetCoreParamsDirty && selectedMatchesCampaign) {
    if (campaign.kitnet_max_size_ae !== undefined && els.kitnetMaxSizeAe) {
      els.kitnetMaxSizeAe.value = String(campaign.kitnet_max_size_ae);
    }
    if (campaign.kitnet_grace_feature_mapping !== undefined && els.kitnetGraceFeatureMapping) {
      els.kitnetGraceFeatureMapping.value = String(campaign.kitnet_grace_feature_mapping);
    }
    if (campaign.kitnet_grace_anomaly_detector !== undefined && els.kitnetGraceAnomalyDetector) {
      els.kitnetGraceAnomalyDetector.value = String(campaign.kitnet_grace_anomaly_detector);
    }
    if (campaign.kitnet_learning_rate !== undefined && els.kitnetLearningRate) {
      els.kitnetLearningRate.value = String(campaign.kitnet_learning_rate);
    }
    if (campaign.kitnet_hidden_ratio !== undefined && els.kitnetHiddenRatio) {
      els.kitnetHiddenRatio.value = String(campaign.kitnet_hidden_ratio);
    }
    if (campaign.kitnet_clip !== undefined && els.kitnetClip) {
      els.kitnetClip.value = String(campaign.kitnet_clip);
    }
  }
  if (!deepantParamsDirty && selectedMatchesCampaign) {
    if (campaign.deepant_estimator && els.deepantEstimator) {
      els.deepantEstimator.value = campaign.deepant_estimator;
    }
    if (campaign.deepant_threshold_window !== undefined && els.deepantWindow) {
      els.deepantWindow.value = String(campaign.deepant_threshold_window);
    }
    if (campaign.deepant_threshold_percentile !== undefined && els.deepantPercentile) {
      els.deepantPercentile.value = String(campaign.deepant_threshold_percentile);
    }
    if (campaign.deepant_anomaly_k_sigma !== undefined && els.deepantKSigma) {
      els.deepantKSigma.value = String(campaign.deepant_anomaly_k_sigma);
    }
    if (campaign.deepant_anomaly_warmup_window !== undefined && els.deepantWarmupWindow) {
      els.deepantWarmupWindow.value = String(campaign.deepant_anomaly_warmup_window);
    }
    if (campaign.deepant_welford_window !== undefined && els.deepantWelfordWindow) {
      els.deepantWelfordWindow.value = String(campaign.deepant_welford_window);
    }
  }
  if (!deepantCoreParamsDirty && selectedMatchesCampaign) {
    const coreBindings = [
      ["deepant_lookback_size", els.deepantLookbackSize],
      ["deepant_initial_window_size", els.deepantInitialWindowSize],
      ["deepant_refit_every", els.deepantRefitEvery],
      ["deepant_window_size", els.deepantWindowSize],
      ["deepant_filters", els.deepantFilters],
      ["deepant_kernel_size", els.deepantKernelSize],
      ["deepant_dropout", els.deepantDropout],
      ["deepant_lr", els.deepantLr],
      ["deepant_epochs_initial", els.deepantEpochsInitial],
      ["deepant_epochs_refit", els.deepantEpochsRefit],
      ["deepant_batch_size", els.deepantBatchSize],
      ["deepant_max_group_workers", els.deepantMaxGroupWorkers],
    ];
    for (const [field, input] of coreBindings) {
      if (campaign[field] !== undefined && input) {
        input.value = String(campaign[field]);
      }
    }
  }
  if (!mdragpointParamsDirty && selectedMatchesCampaign) {
    if (campaign.mdragpoint_verdict_mode && els.mdragpointVerdictMode) {
      els.mdragpointVerdictMode.value = campaign.mdragpoint_verdict_mode;
    }
    if (campaign.mdragpoint_estimator && els.mdragpointEstimator) {
      els.mdragpointEstimator.value = campaign.mdragpoint_estimator;
    }
    if (campaign.mdragpoint_threshold_window !== undefined && els.mdragpointWindow) {
      els.mdragpointWindow.value = String(campaign.mdragpoint_threshold_window);
    }
    if (campaign.mdragpoint_threshold_percentile !== undefined && els.mdragpointPercentile) {
      els.mdragpointPercentile.value = String(campaign.mdragpoint_threshold_percentile);
    }
    if (campaign.mdragpoint_anomaly_k_sigma !== undefined && els.mdragpointKSigma) {
      els.mdragpointKSigma.value = String(campaign.mdragpoint_anomaly_k_sigma);
    }
    if (campaign.mdragpoint_anomaly_warmup_window !== undefined && els.mdragpointWarmupWindow) {
      els.mdragpointWarmupWindow.value = String(campaign.mdragpoint_anomaly_warmup_window);
    }
    if (campaign.mdragpoint_welford_window !== undefined && els.mdragpointWelfordWindow) {
      els.mdragpointWelfordWindow.value = String(campaign.mdragpoint_welford_window);
    }
  }
  if (!mdragpointCoreParamsDirty && selectedMatchesCampaign) {
    const coreBindings = [
      ["mdragpoint_dynamic_threshold", els.mdragpointDynamicThreshold],
      ["mdragpoint_lof_init", els.mdragpointLofInit],
      ["mdragpoint_n_init", els.mdragpointNInit],
      ["mdragpoint_max_nb_clusters", els.mdragpointMaxNbClusters],
      ["mdragpoint_max_cluster_size", els.mdragpointMaxClusterSize],
      ["mdragpoint_lof_neighbors", els.mdragpointLofNeighbors],
    ];
    for (const [field, input] of coreBindings) {
      if (campaign[field] !== undefined && input) {
        if (input.type === "checkbox") {
          input.checked = Boolean(campaign[field]);
        } else {
          input.value = String(campaign[field]);
        }
      }
    }
  }
  syncMdragpointUnifiedFromHidden();
  if (!explainabilityParamsDirty && selectedMatchesCampaign) {
    if (campaign.explainability_nsamples !== undefined && els.explainabilityNsamples) {
      els.explainabilityNsamples.value = String(campaign.explainability_nsamples);
    }
    if (campaign.explainability_v2_nsamples !== undefined && els.explainabilityV2Nsamples) {
      els.explainabilityV2Nsamples.value = String(campaign.explainability_v2_nsamples);
    }
    for (const [field, input] of [
      ["explainability_model", els.explainabilityModel], ["explainability_mode", els.explainabilityMode], ["explainability_method", els.explainabilityMethod],
      ["explainability_v2_model", els.explainabilityV2Model], ["explainability_v2_mode", els.explainabilityV2Mode], ["explainability_v2_method", els.explainabilityV2Method],
    ]) {
      if (input && campaign[field] != null) {
        if (field === "explainability_mode" || field === "explainability_v2_mode") restoreExplainabilityMode(input, String(campaign[field]));
        else if (field === "explainability_model" || field === "explainability_v2_model") restoreExplainabilityModel(input, String(campaign[field]));
        else input.value = String(campaign[field]);
      }
    }
  }
  if (!validationModeDirty && selectedMatchesCampaign) {
    // Deduce the global mode from the campaign flags before restoring them, so
    // updateInteractiveState never clears a just-restored flag. Repeatability
    // wins over Reproducibility (defensive: the backend rejects conflicts).
    // Each rule applies only when the concerned local state is not dirty.
    const repeatabilityEvidence = !repeatabilityDirty && VALIDATION_MODELS.some(
      (model) => Boolean(campaign[`${model}_repeatability`]),
    );
    const reproducibilityEvidence = !reproducibilityDirty && (
      VALIDATION_MODELS.some((model) => Boolean(campaign[`${model}_reproducibility`])) ||
      Boolean(campaign.reproducibility_source_role) ||
      Boolean(campaign.reproducibility_campaign_id)
    );
    applyValidationModeBoxes(
      repeatabilityEvidence ? "repeatability" : reproducibilityEvidence ? "reproducibility" : null,
    );
  }
  if (!repeatabilityDirty && selectedMatchesCampaign) {
    const repeatabilityBindings = [
      ["hstree_repeatability", els.repeatabilityHstree],
      ["iforest_repeatability", els.repeatabilityIforest],
      ["kitnet_repeatability", els.repeatabilityKitnet],
      ["deepant_repeatability", els.repeatabilityDeepant],
      ["mdragpoint_repeatability", els.repeatabilityMdragpoint],
    ];
    for (const [field, input] of repeatabilityBindings) {
      if (campaign[field] !== undefined && input) {
        input.checked = Boolean(campaign[field]);
      }
    }
    updateInteractiveState();
  }
  if (!reproducibilityDirty && selectedMatchesCampaign) {
    const reproducibilityBindings = [
      ["hstree_reproducibility", els.reproducibilityHstree],
      ["iforest_reproducibility", els.reproducibilityIforest],
      ["kitnet_reproducibility", els.reproducibilityKitnet],
      ["deepant_reproducibility", els.reproducibilityDeepant],
      ["mdragpoint_reproducibility", els.reproducibilityMdragpoint],
    ];
    for (const [field, input] of reproducibilityBindings) {
      if (campaign[field] !== undefined && input) {
        input.checked = Boolean(campaign[field]);
      }
    }
    if (campaign.reproducibility_source_role !== undefined && els.reproducibilitySourceRole) {
      els.reproducibilitySourceRole.value = String(campaign.reproducibility_source_role);
    }
    if (campaign.reproducibility_campaign_id !== undefined && els.reproducibilityCampaignId) {
      els.reproducibilityCampaignId.value = String(campaign.reproducibility_campaign_id);
    }
    updateInteractiveState();
  }
}

async function refreshAll() {
  const [status, datasets, campaign, grafana, campaignProgress] = await Promise.all([
    fetchJson(api.status),
    fetchJson(api.datasets),
    fetchJson(api.currentCampaign),
    fetchJson(api.grafanaUrl),
    fetchJson(api.campaignProgress),
  ]);

  currentCampaign = campaign;
  currentPlatformStatus = status?.platform_status || null;
  datasetPresets = datasets.presets || {};
  const previousSelection = els.dataset.value;
  const storedSelection = getStoredDataset();
  const pendingSelection = pendingCampaignPayload?.dataset_path || "";
  els.dataset.innerHTML = "";
  for (const dataset of datasets.datasets) {
    const option = document.createElement("option");
    option.value = dataset;
    option.textContent = datasetPresets[dataset]
      ? `${dataset} [${datasetPresets[dataset].label || "preset"}]`
      : dataset;
    els.dataset.appendChild(option);
  }

  const preferredSelection =
    (pendingSelection && datasets.datasets.includes(pendingSelection) && pendingSelection) ||
    (previousSelection && datasets.datasets.includes(previousSelection) && previousSelection) ||
    (storedSelection && datasets.datasets.includes(storedSelection) && storedSelection) ||
    (!datasetDirty && campaign?.dataset_path && datasets.datasets.includes(campaign.dataset_path) && campaign.dataset_path) ||
    datasets.datasets.find((dataset) => datasetPresets[dataset]) ||
    datasets.datasets[0] ||
    "";
  if (preferredSelection) {
    els.dataset.value = preferredSelection;
    storeDataset(preferredSelection);
  }

  const selectedMatchesCampaign = campaign && (!campaign.dataset_path || els.dataset.value === campaign.dataset_path);
  if (selectedMatchesCampaign) {
    syncFormFromCampaign(campaign);
  } else if (els.dataset.value && !fieldsDirty) {
    applyDatasetPreset(els.dataset.value);
  }

  await Promise.all([
    refreshDatasetTimeRange(els.dataset.value, els.mode.value),
    refreshDatasetPreview(els.dataset.value),
  ]);

  els.grafanaLink.href = grafana.url;
  applyGrafanaLinks(grafana.url);
  if (status?.platform_status) {
    setPlatformState(status.platform_status);
  }
  renderExplainabilityV1Progress(campaignProgress?.explainability_v1);
  renderExplainabilityV2Progress(campaignProgress?.explainability_v2);
  updateInteractiveState();

  if (campaign?.status === "running") {
    console.info("Campaign is running.");
  } else if (!actionInFlight) {
    console.info("Ready to start analysis.");
  }
}

async function postAction(url, payload) {
  const options = {
    method: "POST",
    headers: { "Content-Type": "application/json" },
  };
  if (payload) {
    options.body = JSON.stringify(payload);
  }

  setBusyState(true);

  try {
    await fetchJson(url, options);
    await refreshAll();
  } finally {
    pendingCampaignPayload = null;
    setBusyState(false);
  }
}

function renderExplainabilityV1Progress(view) {
  const card = document.querySelector("#explainability-v1-progress");
  if (!card) {
    return;
  }
  if (!view || !view.selected) {
    card.classList.add("is-hidden");
    return;
  }

  const state = view.state || "processing";
  const stateLabels = { processing: "Processing", complete: "Complete", failed: "Failed" };
  const stateElement = document.querySelector("#explainability-v1-state");
  stateElement.textContent = stateLabels[state] || "Unknown";
  stateElement.className = `badge v2-state-${state}`;
  card.dataset.v1State = state;
  card.classList.remove("is-hidden");

  document.querySelector("#explainability-v1-lag").textContent =
    view.consumer_lag == null ? "Consumer lag: unknown" : `Consumer lag: ${view.consumer_lag}`;

  const recordsElement = document.querySelector("#explainability-v1-records");
  if (view.published_records != null && view.upstream_records != null) {
    recordsElement.textContent = `Published: ${view.published_records} / ${view.upstream_records}`;
  } else {
    recordsElement.textContent = "Published: unknown";
  }

  document.querySelector("#explainability-v1-reason").textContent = view.reason || "-";
}

function renderExplainabilityV2Progress(view) {
  const card = document.querySelector("#explainability-v2-progress");
  if (!card) {
    return;
  }
  if (!view || !view.selected) {
    card.classList.add("is-hidden");
    return;
  }

  const state = view.state || "processing";
  const stateLabels = { processing: "Processing", complete: "Complete", failed: "Failed" };
  const stateElement = document.querySelector("#explainability-v2-state");
  stateElement.textContent = stateLabels[state] || "Unknown";
  stateElement.className = `badge v2-state-${state}`;
  card.dataset.v2State = state;
  card.classList.remove("is-hidden");

  document.querySelector("#explainability-v2-lag").textContent =
    view.consumer_lag == null ? "Consumer lag: unknown" : `Consumer lag: ${view.consumer_lag}`;

  const recordsElement = document.querySelector("#explainability-v2-records");
  if (view.published_records != null && view.upstream_records != null) {
    recordsElement.textContent = `Published: ${view.published_records} / ${view.upstream_records}`;
  } else {
    recordsElement.textContent = "Published: unknown";
  }

  document.querySelector("#explainability-v2-reason").textContent = view.reason || "-";
}

function showExplainabilityV2ForcedWarning() {
  const warning = document.querySelector("#explainability-v2-forced-warning");
  if (warning) {
    warning.classList.remove("is-hidden");
  }
}

els.dataset.addEventListener("change", () => {
  datasetDirty = true;
  storeDataset(els.dataset.value);
  if (datasetPresets[els.dataset.value]) {
    applyDatasetPreset(els.dataset.value);
  } else {
    els.datasetHelp.innerHTML = "Free dataset. No preset from the reference <code>docker-compose</code>.";
    els.datasetHelp.className = "field-help is-neutral";
    if (!fieldsDirty) {
      els.fields.value = "";
    }
    els.fieldsHelp.textContent = "Comma-separated list.";
  }
  refreshDatasetTimeRange(els.dataset.value, els.mode.value);
  refreshDatasetPreview(els.dataset.value);
  updateInteractiveState();
});

els.mode.addEventListener("change", () => {
  modeDirty = true;
  refreshDatasetTimeRange(els.dataset.value, els.mode.value);
  updateInteractiveState();
});

els.fields.addEventListener("input", () => {
  fieldsDirty = true;
  if (currentDatasetPreview) {
    renderDatasetPreview(currentDatasetPreview);
  }
  updateInteractiveState();
});

els.fields.addEventListener("blur", () => {
  els.fields.value = splitCsvLine(els.fields.value).join(", ");
});

for (const cb of [els.modelHstree, els.modelIforest, els.modelKitnet, els.modelDeepant, els.modelMdragpoint, els.modelExplainability, els.modelExplainabilityV2]) {
  if (cb) {
    cb.addEventListener("change", () => {
      modelsDirty = true;
      updateInteractiveState();
    });
  }
}

for (const entry of repeatabilityBoxes()) {
  if (entry.box) {
    entry.box.addEventListener("change", () => {
      repeatabilityDirty = true;
      enforceRepeatabilityExclusivity(entry.model);
      updateInteractiveState();
    });
  }
}

for (const entry of reproducibilityBoxes()) {
  if (entry.box) {
    entry.box.addEventListener("change", () => {
      reproducibilityDirty = true;
      enforceReproducibilityExclusivity(entry.model);
      updateInteractiveState();
    });
  }
}
if (els.reproducibilitySourceRole) {
  els.reproducibilitySourceRole.addEventListener("change", () => {
    reproducibilityDirty = true;
  });
}
if (els.reproducibilityCampaignId) {
  els.reproducibilityCampaignId.addEventListener("input", () => {
    reproducibilityDirty = true;
  });
}

if (els.validationModeRepeatability) {
  els.validationModeRepeatability.addEventListener("change", () => {
    validationModeDirty = true;
    if (els.validationModeRepeatability.checked) {
      // Switching modes clears only the opposite mode's data.
      setValidationMode("repeatability");
    } else {
      // Unchecking the active mode clears its own associated fields.
      resetRepeatabilityFlags();
    }
    updateInteractiveState();
  });
}
if (els.validationModeReproducibility) {
  els.validationModeReproducibility.addEventListener("change", () => {
    validationModeDirty = true;
    if (els.validationModeReproducibility.checked) {
      setValidationMode("reproducibility");
    } else {
      resetReproducibilityFields();
    }
    updateInteractiveState();
  });
}

for (const input of [els.hstreeEstimator, els.hstreeWindow, els.hstreeKSigma, els.hstreeWarmupWindow, els.hstreeWelfordWindow]) {
  if (input) {
    input.addEventListener("change", () => { hstreeParamsDirty = true; updateInteractiveState(); });
    input.addEventListener("input", () => { hstreeParamsDirty = true; });
  }
}
function syncHstreeInitialWindowSize() {
  if (els.hstreeInitialWindowSize && els.hstreeWindowSize) {
    els.hstreeInitialWindowSize.value = els.hstreeWindowSize.value;
  }
}

syncHstreeInitialWindowSize();

for (const input of [els.hstreeNumTrees, els.hstreeMaxDepth, els.hstreeVariant]) {
  if (input) {
    input.addEventListener("change", () => { hstreeCoreParamsDirty = true; });
    input.addEventListener("input", () => { hstreeCoreParamsDirty = true; });
  }
}
if (els.hstreeWindowSize) {
  els.hstreeWindowSize.addEventListener("input", () => {
    hstreeCoreParamsDirty = true;
    syncHstreeInitialWindowSize();
  });
  els.hstreeWindowSize.addEventListener("change", () => {
    hstreeCoreParamsDirty = true;
    syncHstreeInitialWindowSize();
  });
}
for (const input of [els.iforestEstimator, els.iforestKSigma, els.iforestWarmupWindow, els.iforestWindow, els.iforestWelfordWindow]) {
  if (input) {
    input.addEventListener("change", () => { iforestParamsDirty = true; updateInteractiveState(); });
    input.addEventListener("input", () => { iforestParamsDirty = true; });
  }
}
for (const input of [els.iforestInitialWindowSize, els.iforestWindowSize, els.iforestNEstimators, els.iforestMaxSamples, els.iforestRefitEvery, els.iforestContamination]) {
  if (input) {
    input.addEventListener("change", () => { iforestCoreParamsDirty = true; });
    input.addEventListener("input", () => { iforestCoreParamsDirty = true; });
  }
}
for (const input of [els.kitnetEstimator, els.kitnetKSigma, els.kitnetWarmupWindow, els.kitnetWindow, els.kitnetWelfordWindow]) {
  if (input) {
    input.addEventListener("change", () => { kitnetParamsDirty = true; updateInteractiveState(); });
    input.addEventListener("input", () => { kitnetParamsDirty = true; });
  }
}
for (const input of [els.kitnetMaxSizeAe, els.kitnetGraceFeatureMapping, els.kitnetGraceAnomalyDetector, els.kitnetLearningRate, els.kitnetHiddenRatio, els.kitnetClip]) {
  if (input) {
    input.addEventListener("change", () => { kitnetCoreParamsDirty = true; });
    input.addEventListener("input", () => { kitnetCoreParamsDirty = true; });
  }
}
for (const input of [els.deepantEstimator, els.deepantWindow, els.deepantPercentile, els.deepantKSigma, els.deepantWarmupWindow, els.deepantWelfordWindow]) {
  if (input) {
    input.addEventListener("change", () => { deepantParamsDirty = true; updateInteractiveState(); });
    input.addEventListener("input", () => { deepantParamsDirty = true; });
  }
}
for (const input of [els.deepantLookbackSize, els.deepantInitialWindowSize, els.deepantRefitEvery, els.deepantWindowSize, els.deepantFilters, els.deepantKernelSize, els.deepantDropout, els.deepantLr, els.deepantEpochsInitial, els.deepantEpochsRefit, els.deepantBatchSize, els.deepantMaxGroupWorkers]) {
  if (input) {
    input.addEventListener("change", () => { deepantCoreParamsDirty = true; });
    input.addEventListener("input", () => { deepantCoreParamsDirty = true; });
  }
}
for (const input of [els.mdragpointVerdictMode, els.mdragpointEstimator, els.mdragpointWindow, els.mdragpointPercentile, els.mdragpointKSigma, els.mdragpointWarmupWindow, els.mdragpointWelfordWindow]) {
  if (input) {
    input.addEventListener("change", () => { mdragpointParamsDirty = true; updateInteractiveState(); });
    input.addEventListener("input", () => { mdragpointParamsDirty = true; });
  }
}
for (const input of [els.mdragpointDynamicThreshold, els.mdragpointLofInit, els.mdragpointNInit, els.mdragpointMaxNbClusters, els.mdragpointMaxClusterSize, els.mdragpointLofNeighbors]) {
  if (input) {
    input.addEventListener("change", () => { mdragpointCoreParamsDirty = true; });
    input.addEventListener("input", () => { mdragpointCoreParamsDirty = true; });
  }
}

for (const input of [els.explainabilityNsamples, els.explainabilityMode, els.explainabilityMethod, els.explainabilityV2Nsamples, els.explainabilityV2Mode, els.explainabilityV2Method]) {
  if (input) {
    input.addEventListener("change", () => { explainabilityParamsDirty = true; updateInteractiveState(); });
    input.addEventListener("input", () => { explainabilityParamsDirty = true; updateInteractiveState(); });
  }
}
for (const input of [els.explainabilityModel, els.explainabilityV2Model]) {
  if (input) input.addEventListener("change", () => updateInteractiveState());
}

function syncMdragpointUnifiedSelect() {
  // Single visible selector <-> hidden historical controls. The hidden
  // verdict-mode and estimator selects keep the campaign contract fields.
  const unified = els.mdragpointEstimatorUnified;
  const verdictMode = els.mdragpointVerdictMode;
  const estimator = els.mdragpointEstimator;
  if (!unified || !verdictMode || !estimator) {
    return;
  }
  if (unified.value === "native") {
    verdictMode.value = "internal";
  } else {
    verdictMode.value = "external";
    estimator.value = unified.value;
  }
}

function syncMdragpointUnifiedFromHidden() {
  const unified = els.mdragpointEstimatorUnified;
  const verdictMode = els.mdragpointVerdictMode;
  const estimator = els.mdragpointEstimator;
  if (!unified || !verdictMode || !estimator) {
    return;
  }
  unified.value = verdictMode.value === "external" ? (estimator.value || "p99") : "native";
}

if (els.mdragpointEstimatorUnified) {
  els.mdragpointEstimatorUnified.addEventListener("change", () => {
    mdragpointParamsDirty = true;
    syncMdragpointUnifiedSelect();
    updateInteractiveState();
  });
}

syncMdragpointUnifiedFromHidden();

els.form.addEventListener("submit", async (event) => {
  event.preventDefault();
  try {
    if (!els.dataset.value) {
      throw new Error("No dataset selected.");
    }

    const prepared = isCampaignPrepared(currentCampaign, currentPlatformStatus);
    const running = isCampaignRunning(currentCampaign, currentPlatformStatus);

    if (running) {
      throw new Error("Campaign is already running.");
    }

    if (prepared) {
      setPlatformState("ready_for_production");
      const payload = currentFormPayload();
      pendingCampaignPayload = payload;
      await postAction(api.startProduction, payload);
    } else {
      setPlatformState("preparing_platform");
      const payload = currentFormPayload();
      pendingCampaignPayload = payload;
      await postAction(api.launch, payload);
    }
  } catch (error) {
    console.error(error.message);
  }
});

els.stopResetButton.addEventListener("click", async () => {
  setPlatformState("stopping_platform");
  try {
    // First attempt never carries force=true.
    await postAction(api.stopAndReset, null);
  } catch (error) {
    if (!error?.blocked) {
      console.error(error.message);
      return;
    }

    // 409: explainability-v2 is still processing or failed. The stop did not
    // happen, so restore the visual state that was set above instead of
    // leaving the UI on stopping_platform.
    setPlatformState(currentPlatformStatus || "processing_data");
    const warning = `Explainability v2 is ${error.state || "processing"}: ${error.reason || "completion cannot be confirmed"}.`;
    console.warn(warning);

    const confirmed = window.confirm(
      `${warning}\n\nForce stop and reset anyway? ` +
        "The campaign will remain incomplete and v2 results may be missing."
    );
    if (!confirmed) {
      console.warn("Stop and reset cancelled: the campaign remains incomplete.");
      return;
    }

    try {
      // force=true is only built after an explicit user confirmation.
      await postAction(`${api.stopAndReset}?force=true`, null);
      showExplainabilityV2ForcedWarning();
    } catch (forcedError) {
      console.error(forcedError.message);
    }
  }
});

// --- param-info popover ---
// Buttons live inside <label for="…">, so a document-level delegated listener would receive
// a second synthetic click from the label on its associated input, immediately removing
// the popover we just created. Instead we attach per-button listeners and stopPropagation
// so the click never reaches the label.

let _activePopover = null;

function _removePopover() {
  if (_activePopover) {
    _activePopover.remove();
    _activePopover = null;
  }
}

function _showPopover(btn) {
  const pop = document.createElement("div");
  pop.className = "param-info-popover";
  pop.textContent = btn.dataset.info || "";
  pop._source = btn;
  // Append off-screen first so the browser computes its height before we position it.
  pop.style.visibility = "hidden";
  document.body.appendChild(pop);

  const rect = btn.getBoundingClientRect();
  const popW = 260;
  let left = rect.left + rect.width / 2 - popW / 2;
  left = Math.max(8, Math.min(left, window.innerWidth - popW - 8));
  const spaceBelow = window.innerHeight - rect.bottom;
  const top = spaceBelow >= pop.offsetHeight + 12
    ? rect.bottom + 6
    : rect.top - pop.offsetHeight - 6;
  pop.style.left = `${left}px`;
  pop.style.top = `${Math.max(8, top)}px`;
  pop.style.visibility = "";
  _activePopover = pop;
}

document.querySelectorAll(".param-info").forEach(btn => {
  btn.addEventListener("click", e => {
    e.stopPropagation(); // prevent label from firing synthetic click on its associated input
    e.preventDefault();
    if (_activePopover && _activePopover._source === btn) {
      _removePopover();
    } else {
      _removePopover();
      _showPopover(btn);
    }
  });
});

document.addEventListener("click", () => _removePopover());
document.addEventListener("keydown", e => { if (e.key === "Escape") _removePopover(); });
// --- end param-info popover ---

const POLL_DELAY_MS = 10000;
let backgroundRefreshInFlight = null;

// Each progress refresh starts Kafka CLI JVMs inside kafka1: background
// refreshes never overlap and are skipped while the tab is hidden.
function backgroundRefresh() {
  if (backgroundRefreshInFlight === null) {
    backgroundRefreshInFlight = refreshAll()
      .catch(() => {
        // Ignore background refresh errors.
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
  } catch (error) {
    console.error(error.message);
  }
}

initialize();
