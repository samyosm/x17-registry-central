LETTERS = "ABCDEFGH"


def _pmt_channels() -> dict[str, tuple[str, str]]:
    """Map VF48 ADC field names to (PMT label, end) per the TriggerApp channel order."""
    channels = {}
    for end, first_fpga in (("D", 0), ("U", 2)):
        for position in range(16):
            fpga = first_fpga + position // 8
            letter = LETTERS[position % 8]
            scintillator = (16 - position) % 16
            channels[f"FPGA{fpga}_ADC{letter}"] = (f"{end}{scintillator}", end)
    return channels


PMT_CHANNELS = _pmt_channels()
