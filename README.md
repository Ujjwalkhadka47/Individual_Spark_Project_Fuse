International Trade Data Analysis (New Zealand) : 

 # Description of Column_name in datasets 
 Link to dataset : https://www.stats.govt.nz/assets/Uploads/International-trade/International-trade-June-2023-quarter/Download-data/international-trade-june-2023-quarter.zip

- Time_ref: This column represents a timestamp or date reference indicating when the trade transaction occurred.
- Account: This column refer to the account or entity involved in the trade transaction.
- Code: It represents product code.
- Country_Code: This column contains codes representing countries involved in the trade.
- Product type: This column describes the type or category of the traded products.
- Value: This column represents the monetary value associated with the trade transaction.
- Status: The "status" column indicates the status or outcome of the trade transaction (e.g., Free, Charged)


Q.1 Compute the following metrics for each country:
The sum of total export values.
The sum of total import values.
The difference between the sum of export and import values as ‘Trade balance’.
Now, Join  'revised_final" table with the 'country_final' table  & filter the result to select the peak countries with the highest trade balance.

Output : /Users/anujkhadka/Downloads/Individual/images/1.png


Q.2 Show the comparative study of the average transactions of 'account' column (export & import) for different product types (Goods & Services) from 2020 to 2023 combined in a table and show it in columns using Pivot DataFrame Transformations.

Output1 : /Users/anujkhadka/Downloads/Individual/images/2a.png
Output2 : /Users/anujkhadka/Downloads/Individual/images/2b.png
Output3 : /Users/anujkhadka/Downloads/Individual/images/2c.png
Output4 : /Users/anujkhadka/Downloads/Individual/images/2d.png