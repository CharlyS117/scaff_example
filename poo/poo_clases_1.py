class Vehicle:

    def __init__(self,doors = 0,year = 0000,kind = "",brand=""):
        self._doors = doors
        self._years = year
        self._kind = kind
        self._brand = brand

    @staticmethod
    def turn_On():
        print("Run Run")

    @staticmethod
    def claxon():
        print("Mic mic")

class Water(Vehicle):

    def __init__(self):
        self._model = ""

    def get_Model(self, model):
        self._model = model

    def get_Year(self,year):
        self._years = year

    def get_Brand(self,brand):
        self._brand = brand


class Land(Vehicle):

    def __init__(self):
        self._model = ""

    def get_Model(self, model):
        self._model = model

    def get_Year(self, year):
        self._years = year

    def get_Brand(self, brand):
        self._brand = brand



class Tank(Land):
    def __init__(self):
        self._armor = ""
    def get_Armor(self,armor):
        self._armor = armor

class Bus(Land):
    def __init__(self):
        self._kind_engine = ""
    def get_Kind_Engine(self,kind_egine):
        self._kind_engine = kind_egine
class Aircraft(Water):
    def __init__(self):
        self._size = ""
    def get_Size(self,size):
        self._size = size
class Boat(Water):
    def __init__(self):
        self._length = ""
    def get_Length(self,length):
        self._length = length
