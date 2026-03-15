class TechnicalIndicatorFetcherError(Exception):
    pass


class OHLCVValidationError(TechnicalIndicatorFetcherError):
    pass


class IndicatorComputationError(TechnicalIndicatorFetcherError):
    pass


class PatternComputationError(TechnicalIndicatorFetcherError):
    pass


class SerializationError(TechnicalIndicatorFetcherError):
    pass


class MarketDataFetchError(TechnicalIndicatorFetcherError):
    pass
