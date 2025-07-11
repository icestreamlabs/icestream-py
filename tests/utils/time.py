class FakeClock:
    def __init__(self, start=0.0):
        self.now = start
    def advance(self, delta):
        self.now += delta
    def __call__(self):
        return self.now
