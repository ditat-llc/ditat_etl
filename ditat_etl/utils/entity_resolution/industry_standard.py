from copy import deepcopy
import os
from concurrent.futures import ProcessPoolExecutor, as_completed, ThreadPoolExecutor

import spacy

from ...time import TimeIt


class IndustryStandard:
    INDUSTRY_CATEGORIES = {
        "Basic Materials": [
            "Chemicals",
            "Specialty Chemicals",
            "Gold",
            "Building Materials",
            "Aluminum",
            "Coking Coal",
            "Steel",
            "Thermal Coal",
            "Agricultural Inputs",
            "Lumber & Wood Production",
            "Silver",
            "Coal",
            "Uranium",
            "Other Precious Metals & Mining",
            "Paper & Paper Products",
            "Other Industrial Metals & Mining",
            "Copper",
            "Building Products & Equipment",
            "Industrial Metals & Minerals"
        ],
        "Communication Services": [
            "Telecom Services"
        ],
        "Consumer Cyclical": [
            "Auto & Truck Dealerships",
            "Restaurants",
            "Auto Parts",
            "Gambling",
            "Footwear & Accessories",
            "Home Furnishings & Fixtures",
            "Resorts & Casinos",
            "Broadcasting",
            "Specialty Retail",
            "Leisure",
            "Auto Manufacturers",
            "Personal Services",
            "Entertainment",
            "Recreational Vehicles",
            "Media - Diversified",
            "Advertising Agencies",
            "Lodging",
            "Luxury Goods",
            "Broadcasting - Radio",
            "Home Improvement Retail",
            "Textile Manufacturing",
            "Residential Construction",
            "Apparel Retail",
            "Publishing",
            "Furnishings Fixtures & Appliances",
            "Department Stores",
            "Apparel Stores",
            "Packaging & Containers",
            "Apparel Manufacturing",
            "Furnishings",
            "Home Improvement Stores",
            "Broadcasting - TV"
        ],
        "Consumer Defensive": [
            "Household & Personal Products",
            "Packaged Foods",
            "Discount Stores",
            "Beverages - Soft Drinks",
            "Beverages - Brewers",
            "Education & Training Services",
            "Farm Products",
            "Food Distribution",
            "Beverages - Wineries & Distilleries",
            "Tobacco",
            "Beverages - Non-Alcoholic",
            "Confectioners",
            "Pharmaceutical Retailers",
            "Grocery Stores"
        ],
        "Energy": [
            "Oil & Gas Integrated",
            "Oil & Gas Drilling",
            "Oil & Gas Refining & Marketing",
            "Oil & Gas Midstream",
            "Oil & Gas E&P",
            "Oil & Gas Equipment & Services"
        ],
        "Financial Services": [
            "Financial Exchanges",
            "Insurance - Life",
            "Insurance Brokers",
            "Capital Markets",
            "Insurance - Specialty",
            "Financial Conglomerates",
            "Asset Management",
            "Specialty Finance",
            "Banks - Global",
            "Insurance - Reinsurance",
            "Mortgage Finance",
            "Credit Services",
            "Financial Data & Stock Exchanges",
            "Insurance - Diversified",
            "Banks - Diversified",
            "Savings & Cooperative Banks",
            "Banks - Regional - US",
            "Banks - Regional",
            "Insurance - Property & Casualty"
        ],
        "Healthcare": [
            "Healthcare Plans",
            "Diagnostics & Research",
            "Medical Devices",
            "Drug Manufacturers - Specialty & Generic",
            "Health Care Plans",
            "Medical Distribution",
            "Medical Care Facilities",
            "Medical Care",
            "Medical Instruments & Supplies",
            "Drug Manufacturers - General",
            "Biotechnology",
            "Drug Manufacturers - Major",
            "Long-Term Care Facilities"
        ],
        "Industrials": [
            "Waste Management",
            "Business Services",
            "Trucking",
            "Airports & Air Services",
            "Airlines",
            "Conglomerates",
            "Industrial Distribution",
            "Infrastructure Operations",
            "Railroads",
            "Diversified Industrials",
            "Rental & Leasing Services",
            "Marine Shipping",
            "Specialty Industrial Machinery",
            "Staffing & Employment Services",
            "Farm & Construction Equipment",
            "Pollution & Treatment Controls",
            "Aerospace & Defense",
            "Specialty Business Services",
            "Electrical Equipment & Parts",
            "Tools & Accessories",
            "Shell Companies",
            "Engineering & Construction",
            "Staffing & Outsourcing Services",
            "Security & Protection Services",
            "Farm & Heavy Construction Machinery",
            "Integrated Freight & Logistics",
            "Shipping & Ports",
            "Metal Fabrication",
            "Travel Services",
            "Business Equipment & Supplies"
        ],
        "Real Estate": [
            "REIT - Diversified",
            "REIT - Mortgage",
            "Real Estate - Development",
            "REIT - Office",
            "Real Estate Services",
            "REIT - Specialty",
            "REIT - Industrial",
            "Real Estate - General",
            "REIT - Hotel & Motel",
            "REIT - Retail",
            "REIT - Healthcare Facilities",
            "Real Estate - Diversified",
            "REIT - Residential"
        ],
        "Technology": [
            "Computer Hardware",
            "Internet Retail",
            "Communication Equipment",
            "Electronic Components",
            "Information Technology Services",
            "Scientific & Technical Instruments",
            "Health Information Services",
            "Semiconductor Equipment & Materials",
            "Solar",
            "Software - Infrastructure",
            "Data Storage",
            "Internet Content & Information",
            "Software - Application",
            "Consumer Electronics",
            "Electronic Gaming & Multimedia",
            "Semiconductors",
            "Computer Systems",
            "Electronics & Computer Distribution",
            "Semiconductor Memory"
        ],
        "Utilities": [
            "Utilities - Diversified",
            "Utilities - Independent Power Producers",
            "Utilities - Regulated Gas",
            "Utilities - Renewable",
            "Utilities - Regulated Electric",
            "Utilities - Regulated Water"
        ]
    }

    @TimeIt()
    def __init__( self, english_pipeline: str='en_core_web_lg') -> None:
        self.load_nlp(english_pipeline=english_pipeline)
        self.industry_nlp = [self.nlp(word) for word in self.industry_categories]
        self.subindustry_nlp = [self.nlp(word) for word in [item for sublist in self.industry_categories.values() for item in sublist]]

    def load_nlp(self, english_pipeline):
        try:
            self.nlp = spacy.load(english_pipeline)

        except Exception:
            os.system(f"python -m spacy download {english_pipeline}")
            self.nlp = spacy.load(english_pipeline)

    @property
    def industry_categories(self):
        industry_categories = deepcopy(type(self).INDUSTRY_CATEGORIES)

        # Putting back the parent in the options for matching.
        for industry in industry_categories:
            industry_categories[industry].insert(0, industry)

        return industry_categories
        
    # @TimeIt()
    def classify(self, text, th=0.0, verbose=True):
        if verbose:
            print(f'Classifying: {text}')

        text_nlp = self.nlp(text)

        results = {}
        
        for subindustry in self.subindustry_nlp:
            similarity = subindustry.similarity(text_nlp)
            results[subindustry.text] = similarity

        subindustry_result = max(results, key=results.get)
        score = results[subindustry_result]

        for k, v in self.industry_categories.items():
            if subindustry_result in v:
                industry_result = k
                break

        # in case industry and subindustry are the same, we go for 2nd best for subindustry
        if subindustry_result == industry_result:
            filtered_results = {
                i: j for i, j in results.items() if i != industry_result \
                and i in self.industry_categories[industry_result]
            }
            subindustry_result = max(filtered_results, key=filtered_results.get)
            score = filtered_results[subindustry_result]

        if score < th:
            return (None, None, None)

        return (industry_result, subindustry_result, score)

    @TimeIt()
    def classify_bulk(self, text: str or list, th=0.0, verbose=True):
        text = [text] if isinstance(text, str) else text

        unique_text = set(text)

        unique_results = {i: self.classify(i, verbose=verbose, th=th) for i in unique_text}

        results = [unique_results[i] for i in text]

        return results


if __name__ == '__main__':
    c = IndustryStandard()

