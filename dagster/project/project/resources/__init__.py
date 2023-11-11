import time
from dataclasses import asdict, dataclass
from typing import List

import requests
from pydantic import Field

from dagster import ConfigurableResource


@dataclass
class Salary:
    country: str
    id: str
    title: str
    p25: float
    p50: float
    p75: float
    loaded_at: int

    def to_dict(self) -> dict:
        props = {k: v for k, v in asdict(self).items() if not k.startswith("_")}
        return props

    def properties(self):
        return (
            self.country,
            self.id,
            self.title,
            self.p25,
            self.p50,
            self.p75,
            self.loaded_at,
        )

    def __eq__(self, other):
        if type(other) is type(self):
            return self.properties() == other.properties()
        else:
            return False

    def __hash__(self):
        return hash(self.properties())

    def __getitem__(self, key):
        return getattr(self, key)


class SalaryAPI:
    def __init__(self):
        self.url_fstring = (
            "https://api.teleport.org/api/countries/iso_alpha2:{country}/salaries/"
        )

    def get_salaries_for_country(self, country: str) -> List[Salary]:
        req = requests.get(self.url_fstring.format(country=country))
        req.raise_for_status()
        salaries = req.json()["salaries"]

        results = []
        for salary in salaries:
            s = Salary(
                country,
                salary["job"]["id"],
                salary["job"]["title"],
                salary["salary_percentiles"]["percentile_25"],
                salary["salary_percentiles"]["percentile_50"],
                salary["salary_percentiles"]["percentile_75"],
                int(time.time()),
            )
            results.append(s)

        return results


class SalaryAPIResource(ConfigurableResource):
    @property
    def fetch(self) -> SalaryAPI:
        return SalaryAPI()

    def get_salaries(self, country) -> List[Salary]:
        return self.fetch.get_salaries_for_country(country)

    def get_salaries_for_countries(self) -> List[Salary]:
        result = []
        for country in self.countries.split(","):
            result.extend(self.fetch.get_salaries_for_country(country))
        return result
