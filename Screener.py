import requests
from bs4 import BeautifulSoup
import pandas as pd
import re

def scrape_quarterly_results(stock_code):
    #url = f"https://www.screener.in/company/{stock_code}/consolidated/"
    url = f"https://www.screener.in/company/{stock_code}/"
    
    response = requests.get(url)
    
    if response.status_code == 200:
        soup = BeautifulSoup(response.content, 'html.parser')
        table = soup.find('table', {'class': 'data-table responsive-text-nowrap'})
        
        if table:
            headers = [th.text.strip() for th in table.find_all('th')]
            data = []
            for row in table.find_all('tr')[1:]:
                row_data = [td.text.strip() for td in row.find_all('td')]
                data.append(row_data)
            
            df = pd.DataFrame(data, columns=headers)
            return df
        else:
            print(f"Quarterly results table not found for {stock_code}.")
            return None
    else:
        print(f"Failed to retrieve the webpage for {stock_code}. Status code: {response.status_code}")
        return None

# List of stock codes
stock_codes = [
'TOLINS',
'AVANTEL']  # Add or modify stock codes as needed

# Dictionary to store DataFrames
stock_dataframes = {}

# Loop through the stock codes
for stock_code in stock_codes:
    print(f"Scraping data for {stock_code}...")
    results = scrape_quarterly_results(stock_code)
    
    if results is not None:
        # Store the DataFrame in the dictionary with the stock code as the key
        stock_dataframes[stock_code] = results
        print(f"Data for {stock_code} successfully scraped and stored.")
        
        # Optionally, save to CSV
        results.to_csv(f"{stock_code}_quarterly_results.csv", index=False)
        print(f"Data for {stock_code} saved to CSV.")
    else:
        print(f"Failed to scrape data for {stock_code}.")
    
    print("------------------------")

def extract_metric(df, metric_names):
    if df is None or df.empty:
        print(f"Warning: DataFrame is empty or None for metrics {metric_names}")
        return None
    
    for metric_name in metric_names:
        pattern = re.compile(f"{metric_name}\\s*\\+?", re.IGNORECASE)
        matching_rows = df[df.iloc[:, 0].str.contains(pattern, na=False, regex=True)]
        
        if not matching_rows.empty:
            return matching_rows.iloc[0].tolist()
    
    print(f"Warning: Metrics {metric_names} not found in DataFrame")
    return None

def create_summary_dataframe(stock_dataframes, metric_names):
    all_data = []
    all_quarters = set()

    for stock_code, df in stock_dataframes.items():
        if df is not None and not df.empty:
            metric_data = extract_metric(df, metric_names)
            if metric_data:
                quarters = df.columns.tolist()
                all_quarters.update(quarters)
                data_dict = dict(zip(quarters, metric_data))
                all_data.append({
                    'Stock Code': stock_code,
                    'Metric': ' / '.join(metric_names),
                    'Data': data_dict
                })
                print(f"Debug: {stock_code} - {' / '.join(metric_names)} data: {data_dict}")
            else:
                print(f"Warning: Metrics {metric_names} not found for {stock_code}")
        else:
            print(f"Warning: No data found for {stock_code}")

    # Sort quarters chronologically
    all_quarters = sorted(list(all_quarters))

    # Create the final summary DataFrame
    summary_data = []
    for item in all_data:
        row = [item['Stock Code']]
        for quarter in all_quarters:
            row.append(item['Data'].get(quarter, None))
        summary_data.append(row)

    return pd.DataFrame(summary_data, columns=['Stock Code'] + all_quarters)

# Debugging function to print DataFrame details
def print_df_details(df, name):
    print(f"\n{name} DataFrame:")
    print(f"Shape: {df.shape}")
    print(f"Columns: {df.columns.tolist()}")
    print(df)
    print("\nData types:")
    print(df.dtypes)
    print("\nNon-null counts:")
    print(df.count())

# Print the first few rows of each stock's DataFrame for debugging
print("\nDebugging: Individual stock DataFrames")
for stock_code, df in stock_dataframes.items():
    print(f"\nFull {stock_code} DataFrame:")
    print(df)
    print("------------------------")

# Create summary DataFrames for Sales/Revenue, Net Profit, and Operating Profit
print("\nCreating Sales/Revenue summary...")
sales_df = create_summary_dataframe(stock_dataframes, ['Sales', 'Revenue'])
print("\nCreating Net Profit summary...")
net_profit_df = create_summary_dataframe(stock_dataframes, ['Net Profit'])
print("\nCreating Operating Profit summary...")
operating_profit_df = create_summary_dataframe(stock_dataframes, ['Operating Profit'])

# Print debugging information
print_df_details(sales_df, "Sales/Revenue")
print_df_details(net_profit_df, "Net Profit")
print_df_details(operating_profit_df, "Operating Profit")

# Optionally, save the summary DataFrames to CSV
sales_df.to_csv('sales_revenue_summary.csv', index=False)
net_profit_df.to_csv('net_profit_summary.csv', index=False)
operating_profit_df.to_csv('operating_profit_summary.csv', index=False)
