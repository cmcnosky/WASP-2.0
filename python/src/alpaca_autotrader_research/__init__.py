"""Clean-room research orchestration with no Python trading-decision fallback."""

from .certification import (
    BootstrapLcbResult,
    DeflatedSharpeResult,
    DerivedCertificationStatistics,
    InsufficientEvidenceError,
    PboResult,
    TrackRecordResult,
    deflated_sharpe_probability,
    derive_certification_statistics,
    moving_block_bootstrap_annualized_lcb,
    probability_of_backtest_overfitting,
    track_record_diagnostics,
)
from .core_bridge import (
    CoreBridge,
    CoreBridgeError,
    CoreInvocationError,
    CoreProtocolError,
    CoreUnavailableError,
)
from .gates import (
    DerivedGateReport,
    GateEvidence,
    GateReport,
    GateThresholds,
    evaluate_gates,
    evaluate_gates_from_core_outputs,
)
from .protocol import LOCKED_SPLITS, generate_preregistration

__all__ = [
    "BootstrapLcbResult",
    "CoreBridge",
    "CoreBridgeError",
    "CoreInvocationError",
    "CoreProtocolError",
    "CoreUnavailableError",
    "DeflatedSharpeResult",
    "DerivedCertificationStatistics",
    "DerivedGateReport",
    "GateEvidence",
    "GateReport",
    "GateThresholds",
    "InsufficientEvidenceError",
    "LOCKED_SPLITS",
    "PboResult",
    "TrackRecordResult",
    "deflated_sharpe_probability",
    "derive_certification_statistics",
    "evaluate_gates",
    "evaluate_gates_from_core_outputs",
    "generate_preregistration",
    "moving_block_bootstrap_annualized_lcb",
    "probability_of_backtest_overfitting",
    "track_record_diagnostics",
]

__version__ = "0.1.0"
