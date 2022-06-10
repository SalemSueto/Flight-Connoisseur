# Flight-Connoisseur
The present script searches for available flights between two cities. The user needs to provide two inputs.

The first input is a text file (i.e. flight_data.txt) with the following structure:

| From_City | From_Country | From_IATA | To_City | To_Country | To_IATA | Max_Transfer | Max_Price | 
|-----------|--------------|-----------|---------|------------|---------|--------------|-----------| 
| Berlin    | Germany      | BER       | Baku    | Azerbaijan |         | 2            | 200       | 
| Berlin    | Germany      | BER       | Yerevan | Armenia    | EVN     | 2            | 80        | 
| Frankfurt | Germany      |           | Tokyo   | Japan      | TYO     | 2            | 100       | 
| Amsterdam | Netherlands  | AMS       | Accra   | Ghana      |         | 2            | 350       | 

The "From_IATA" and the "To_IATA" columns are not mandatory to be filled. The script will update them in case they are missing.
In case a city has different IATA codes then the one with the best world ranking will be saved for searching analysis.
Each row will be used as input data to select flights from the present day to the following 6 months.

The second input is the "tequila_api" that can be obtained by following the next instructions.
* Registration at https://partners.kiwi.com/
* Once in the homepage, click on "My solutions" then on the "Create solution" button. Choose "Meta Search API integration". 
  Then, click on the "One-way and Return", and click on the "Next" button. Finally, insert a solution name in the text field and click on "Create".

The personal API code can be found by clicking on the created solution and searching for the text field linked to the label called "API key".

Finally, the Kiwi API requests were obtained at https://tequila.kiwi.com/portal/docs/tequila_api.
