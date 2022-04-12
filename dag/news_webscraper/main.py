from NewsScraper import NewsScraper

if __name__ == "__main__":
    tickers = ["D05-SG", "O39-SG", "U11-SG", "Z74-SG", "J36-SG", "C38U-SG", "9CI-SG", "A17U-SG", "F34-SG", "BN4-SG"]
    companies = ["DBS Group Holdings Ltd", "Oversea-Chinese Banking Corporation Limited", "United Overseas Bank Ltd. (Singapore)", "Singapore Telecommunications Limited", "Jardine Matheson Holdings Limited", "CapitaLand Integrated Commercial Trust", "CapitaLand Investment Limited", "Ascendas Real Estate Investment Trust", "Wilmar International Limited", "Keppel Corporation Limited"]
    NewsScraper.scrape_news(tickers, companies)
