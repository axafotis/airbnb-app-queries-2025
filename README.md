Database Application - Airbnb Project
Function Modifications

+1. checkIfPropertyExists

    Changes:

    The function’s return type was replaced from a simple string ("yes", "no") to a list of tuples ([('yes',)], [('no',)]), as required by the assignment for compatibility with the Bottle framework.

    The use of COUNT(*) > 0 was replaced with COUNT(*) to avoid boolean results, which were not allowed according to the instructions.

    The query was reformulated to use simple joins, avoiding the forbidden JOIN syntax.

+2. selectTopNhosts

    Changes:

    The syntax using GROUP BY and ORDER BY was replaced with a manual mechanism for finding the top N hosts for each property type, avoiding the forbidden use of LIMIT and JOIN.

    Added checks for empty property sets to prevent unnecessary queries.

    Used sorted() with slicing to select the top N hosts while preserving descending order.

    Added explicit conversion of N to an integer to prevent type errors.

+3. findMatchingProperties

    Changes:

    Replaced the use of JOIN with analytical queries for data collection, avoiding forbidden joins.

    Added a more efficient handling of blocked hosts using sets for faster lookups.

    Moved the filtering of blocked hosts from SQL to Python for better performance.

    Used a dictionary (property_host_map) for faster host lookups per property.

+4. countWordsForProperties

    Changes:

    Redesigned the property filtering logic to avoid multiple nested queries and forbidden JOINs.

    Added the use of dictionaries (guests_per_property, property_host_map) for faster data retrieval without multiple database queries.

    Removed the use of GROUP BY and HAVING to comply with assignment restrictions.

    Implemented a more efficient stop-word filtering and word frequency calculation mechanism for reviews.

    Review text processing was done entirely in Python using regular expressions (re) and counters (Counter) to avoid excessive database queries and improve performance.

+5. findCommonPropertiesAndGuests

    Changes:

    Removed the use of JOIN from SQL queries to comply with assignment restrictions and improved the individual guest search logic.

    Used set-based filtering in Python (set.discard()) to remove guest IDs a and b without additional queries.

    Added a mechanism to avoid duplicate results using a set (checked_pairs).

    Moved the check for common properties between guests to Python for better performance and flexibility in result management.

+6. highValueHost

    Changes:

    Replaced the use of GROUP BY and nested SQL queries with Python dictionaries for more efficient host statistics management.

    Added validation for invalid input parameters using Decimal() for more reliable numeric handling.

    Implemented data collection structures (host_prices, host_ratings, host_counts) for efficient calculation of average price and rating, avoiding multiple queries.

    Removed the use of JOIN in final queries for finding the most frequent amenities, using dictionaries (amenity_id_to_name) instead for fast mapping.

+7. recommendProperty

    Changes:

    Added validation for invalid desired_amenities format using json.loads() for flexible input handling.

    Replaced the property selection logic with a scoring mechanism implemented in Python using dictionaries for efficient weighted score calculation.

    Removed the use of nested queries for amenities and replaced them with bulk queries for better performance.

    Used a sorted list to select the highest-scoring property, avoiding multiple loops and redundant queries.

This project was developed as a university assignment for a Database Systems course.
The connection to the original MySQL database (university instance) is no longer active,
so the code can only be executed using your own MySQL database or mock data.