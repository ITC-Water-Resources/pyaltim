class APILimitReached(Exception):
    """Exception raised iwhen API rates are saturated

    Attributes:
        message -- explanation of the error
    """

    def __init__(self, message):
        self.message = message
        super().__init__(self.message)
