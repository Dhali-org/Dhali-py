import pytest

import dhali.transaction_utils as dtx

__author__ = "Dhali-org"
__copyright__ = "Dhali-org"
__license__ = "MIT"


def test_rate_converters_dollars_to_xrp():
    """Test Dollar to XRP converter"""
    assert dtx.convert_dollars_to_xrp(0) == 0
    assert dtx.convert_dollars_to_xrp(1) == 2.5
    assert dtx.convert_dollars_to_xrp(2.234) == pytest.approx(2.234 * 2.5, 1e-8)
    with pytest.raises(ValueError):
        dtx.convert_dollars_to_xrp(-1)

def test_determine_cost_dollars():
    """Test determine instance cost in dollars"""
    fudge_factor = 5
    GiB_s_dollars_price = (
        0.000002905 * fudge_factor
    )
    GiB_memory = 1  # TODO 'machine_type' should determine this
    assert dtx.determine_cost_dollars(machine_type = "TODO", runtime_ms = 20, request_size_bytes = 2, response_size_bytes = 3) == pytest.approx(GiB_s_dollars_price * GiB_memory * 20 * 2 * 3 / 1000)
    assert dtx.determine_cost_dollars(machine_type = "TODO", runtime_ms = 20, request_size_bytes = 1.982, response_size_bytes = 1.7835) == pytest.approx(GiB_s_dollars_price * GiB_memory * 20 * 1.982 * 1.7835 / 1000)
    with pytest.raises(ValueError):
        dtx.determine_cost_dollars(machine_type = "TODONT", runtime_ms = 20, request_size_bytes = 2, response_size_bytes = 3)
    with pytest.raises(ValueError):
        dtx.determine_cost_dollars(machine_type = "TODO", runtime_ms = -1, request_size_bytes = 2, response_size_bytes = 3)
    with pytest.raises(ValueError):
        dtx.determine_cost_dollars(machine_type = "TODO", runtime_ms = 20, request_size_bytes = -2, response_size_bytes = 3)
    with pytest.raises(ValueError):
        dtx.determine_cost_dollars(machine_type = "TODO", runtime_ms = 20, request_size_bytes = 2, response_size_bytes = -3)
