
class Direct:
    FlyFrom: str
    FlyTo: str
    Country: str
    CountryCode: str
    City: str
    DisplayName: str
    Latitude: str
    Longitude: str
    NumberOfRoutes: str

    def __init__(self, fly_from, fly_to, country, city, display_name, latitude, longitude, number_of_routes):
        self.FlyFrom = fly_from
        self.FlyTo = fly_to
        self.Country = country
        self.City = city
        self.DisplayName = display_name
        self.Latitude = latitude
        self.Longitude = longitude
        self.NumberOfRoutes = number_of_routes


    def to_dict(self):
        return {
            'FlyFrom': self.FlyFrom,
            'FlyTo': self.FlyTo,
            'Country': self.Country,
            'City': self.City,
            'DisplayName': self.DisplayName,
            'Latitude': self.Latitude,
            'Longitude': self.Longitude,
            'NumberOfRoutes': self.NumberOfRoutes
        }