from .base import BaseParser

class ResultsParser(BaseParser):
    def __init__(self):
        super().__init__(base_url="https://www.hltv.org/results")
    
    def parse(self):
        """Parse match results page"""
        filepath = self.get_page(self.base_url, "results")
        # Here you can add specific parsing logic for results page
        return filepath
