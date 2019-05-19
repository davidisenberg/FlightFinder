
class Direct:
    FlyFrom: str
    FlyTo: str

    def __init__(self, fly_from, fly_to):
        self.FlyFrom = fly_from
        self.FlyTo = fly_to

    def to_dict(self):
        return {
            'FlyFrom': self.FlyFrom,
            'FlyTo': self.FlyTo,
        }