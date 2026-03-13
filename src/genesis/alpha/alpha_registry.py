class AlphaRegistry:
    def __init__(self):
        self._registry = {}

    def register(self, name, alpha):
        self._registry[name] = alpha

    def get(self, name):
        return self._registry[name]

    def list_names(self):
        return list(self._registry)
