from .base import BaseParser

class MatchesParser(BaseParser):
    def __init__(self):
        super().__init__(base_url="https://www.hltv.org/matches")
    
    def parse(self):
        """Parse upcoming matches page"""
        filepath = self.get_page(self.base_url, "matches")
        # Here you can add specific parsing logic for matches page
        return filepath
