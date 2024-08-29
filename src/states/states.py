import pandas as pd


class State:
    name: str
    abbreviation: str
    fips: str

    def __init__(self, name: str, abbreviation: str, fips: str):
        self.name = name
        self.abbreviation = abbreviation
        self.fips = fips

    def __dict__(self):
        return {"name": self.name, "abbreviation": self.abbreviation, "fips": self.fips}

    def __str__(self):
        return str(self.__dict__())

    def get_fips(self):
        return self.fips

    def get_name(self):
        return self.name

    def get_abbreviation(self):
        return self.abbreviation


Alabama = State('Alabama', 'AL', '01')
Alaska = State('Alaska', 'AK', '02')
Arizona = State('Arizona', 'AZ', '04')
Arkansas = State('Arkansas', 'AR', '05')
California = State('California', 'CA', '06')
Colorado = State('Colorado', 'CO', '08')
Connecticut = State('Connecticut', 'CT', '09')
Delaware = State('Delaware', 'DE', '10')
DistrictOfColumbia = State('District Of Columbia', 'DC', '11')
Florida = State('Florida', 'FL', '12')
Georgia = State('Georgia', 'GA', '13')
Hawaii = State('Hawaii', 'HI', '15')
Idaho = State('Idaho', 'ID', '16')
Illinois = State('Illinois', 'IL', '17')
Indiana = State('Indiana', 'IN', '18')
Iowa = State('Iowa', 'IA', '19')
Kansas = State('Kansas', 'KS', '20')
Kentucky = State('Kentucky', 'KY', '21')
Louisiana = State('Louisiana', 'LA', '22')
Maine = State('Maine', 'ME', '23')
Maryland = State('Maryland', 'MD', '24')
Massachusetts = State('Massachusetts', 'MA', '25')
Michigan = State('Michigan', 'MI', '26')
Minnesota = State('Minnesota', 'MN', '27')
Mississippi = State('Mississippi', 'MS', '28')
Missouri = State('Missouri', 'MO', '29')
Montana = State('Montana', 'MT', '30')
Nebraska = State('Nebraska', 'NE', '31')
Nevada = State('Nevada', 'NV', '32')
NewHampshire = State('New Hampshire', 'NH', '33')
NewJersey = State('New Jersey', 'NJ', '34')
NewMexico = State('New Mexico', 'NM', '35')
NewYork = State('New York', 'NY', '36')
NorthCarolina = State('North Carolina', 'NC', '37')
NorthDakota = State('North Dakota', 'ND', '38')
Ohio = State('Ohio', 'OH', '39')
Oklahoma = State('Oklahoma', 'OK', '40')
Oregon = State('Oregon', 'OR', '41')
Pennsylvania = State('Pennsylvania', 'PN', '42')
PuertoRico = State('Puerto Rico', 'PR', '72')
RhodeIsland = State('Rhode Island', 'RI', '44')
SouthCarolina = State('South Carolina', 'SC', '45')
SouthDakota = State('South Dakota', 'SD', '46')
Tennessee = State('Tennessee', 'TN', '47')
Texas = State('Texas', 'TX', '48')
Utah = State('Utah', 'UT', '49')
Vermont = State('Vermont', 'VT', '50')
Virginia = State('Virginia', 'VA', '51')
VirginIslands = State("Virgin Islands", "VI", "78")
Washington = State('Washington', 'WA', '53')
WestVirgina = State('West Virgina', 'WV', '54')
Wisconsin = State('Wisconsin', 'WI', '55')
Wyoming = State('Wyoming', 'WY', '56')


def generate_state_list() -> list:
    """

    :return: A list of all possible states as dictionaries
    """
    states: list = [Alabama, Alaska, Arizona, Arkansas, California, Colorado, Connecticut, Delaware,
                        DistrictOfColumbia, Florida, Georgia, Hawaii, Idaho, Illinois, Indiana, Iowa, Kansas,
                        Kentucky, Louisiana, Maine, Maryland, Massachusetts, Michigan, Minnesota, Mississippi,
                        Missouri, Montana, Nebraska, Nevada, NewHampshire, NewJersey, NewMexico, NewYork, NorthCarolina,
                        NorthDakota, Ohio, Oklahoma, Oregon,Pennsylvania, PuertoRico, RhodeIsland, SouthCarolina,
                        SouthDakota, Tennessee, Texas, Utah, Vermont, Virginia, Washington, WestVirgina, Wisconsin,
                        Wyoming]

    return [state.__dict__() for state in states]


state_list: list = generate_state_list()

state_df: pd.DataFrame = pd.DataFrame(state_list)


def get_states_list() -> list:
    """

    :return: Returns the states in the continental us and outlying regions as a list of dictionaries
    """
    return state_list


def get_states_df() -> pd.DataFrame:
    """

    :return: Returns the states in the continental us and outlying regions as a pandas dataframe
    """
    return state_df