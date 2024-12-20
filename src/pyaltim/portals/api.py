class APILimitReached(Exception):
    """Exception raised iwhen API rates are saturated

    Attributes:
        message -- explanation of the error
    """

    def __init__(self, message):
        self.message = message
        super().__init__(self.message)

class APIDataNotFound(Exception):
    """Exception raised when API reqeusts did not return useful data

    Attributes:
        message -- explanation of the error
    """

    def __init__(self, message):
        self.message = message
        super().__init__(self.message)


class APIOtherError(Exception):
    """Exception raised when auxiliary errors occurred

    Attributes:
        message -- explanation of the error
    """

    def __init__(self, message):
        self.message = message
        super().__init__(self.message)
