# ----- CONFIGURE YOUR EDITOR TO USE 4 SPACES PER TAB ----- #
import pymysql
import os
from dotenv import load_dotenv 

load_dotenv()
# Example usage:
#db_config = {
 #   'host': 'db.gntserver1.freeddns.org',
 #   'user': 'fotis', ##your user name here, usually root
  #  'password': 'fotis1234', ##your password here
   # 'database': 'Airbnb', ## the name of your database
    # 'charset': 'utf8mb4', #optional
    # 'cursorclass': pymysql.cursors.DictCursor #optional
#}

# Τα στοιχεία σύνδεσης φορτώνονται από το αρχείο .env για ασφάλεια
# (δεν βάζουμε credentials μέσα στον κώδικα)
db_config = {
    'host': os.getenv('DB_HOST', '127.0.0.1'),
    'user': os.getenv('DB_USER', ''),
    'password': os.getenv('DB_PASS', ''),
    'database': os.getenv('DB_NAME', ''),
}


""" By default, pymysql cursors return results as tuples. Each tuple represents a row from the database, 
    and you access the columns by their numerical index (e.g., row[0], row[1]).
    pymysql.cursors.DictCursor changes this behavior. Instead of tuples, it returns results as dictionaries.
"""

def checkIfPropertyExists(location_a, property_type_a):
    connection = pymysql.connect(**db_config)
    cursor = connection.cursor()

    try:
        query = (
            "SELECT COUNT(*) "  # Μετράμε πόσες γραμμές ικανοποιούν το φίλτρο
            "FROM property p, propertytype pt, property_has_type pht "
            "WHERE p.property_id = pht.property_id "
            "AND pt.type_id = pht.type_id "
            "AND p.location = %s "
            "AND pt.type_name = %s;"
        )

        cursor.execute(query, (location_a, property_type_a))
        count = cursor.fetchone()[0]

        if count > 0:
            return [("exists",),("yes",)]
        else:
            return [("exists",),("no",)]
    finally:
        cursor.close()
        connection.close()




def selectTopNhosts(N):
    connection = pymysql.connect(**db_config)
    cursor = connection.cursor()
    
     ### YOUR CODE HERE
    try:
        # Μετατροπή του N σε ακέραιο για να αποφύγουμε σφάλματα με slice
        N = int(N)

        # 1. Ανάκτηση όλων των τύπων καταλυμάτων
        cursor.execute("SELECT type_id, type_name FROM propertytype")
        property_types = cursor.fetchall()

        # Δημιουργία λίστας για τα τελικά αποτελέσματα
        results = [("Property Type", "Host ID", "Property Count")]

        # 2. Εύρεση των top N ιδιοκτητών για κάθε τύπο καταλύματος
        for type_id, type_name in property_types:
            # Βρίσκουμε όλα τα property_id για τον συγκεκριμένο τύπο καταλύματος
            cursor.execute("SELECT property_id FROM property_has_type WHERE type_id = %s", (type_id,))
            property_ids = [row[0] for row in cursor.fetchall()]

            # Αν δεν υπάρχουν καταλύματα για αυτόν τον τύπο, προχωράμε στον επόμενο τύπο
            if not property_ids:
                continue

            # Βρίσκουμε τους hosts για αυτά τα properties χωρίς JOIN
            host_counts = {}
            for property_id in property_ids:
                cursor.execute("SELECT host_id FROM property WHERE property_id = %s", (property_id,))
                host_id = cursor.fetchone()

                if host_id is not None:
                    host_id = host_id[0]
                    if host_id in host_counts:
                        host_counts[host_id] += 1
                    else:
                        host_counts[host_id] = 1

            # Φιλτράρισμα των top N ιδιοκτητών (χωρίς χρήση LIMIT)
            top_hosts = sorted(host_counts.items(), key=lambda x: x[1], reverse=True)[:N]

            # Προσθήκη των αποτελεσμάτων στη λίστα αποτελεσμάτων
            for host_id, count in top_hosts:
                results.append((type_name, host_id, count))

        # Επιστροφή των αποτελεσμάτων με τη μορφή που ζητάει η εκφώνηση
        return results

    except Exception as e:
        print(f"Error executing selectTopNhosts: {e}")  # Εκτύπωση μηνύματος λάθους σε περίπτωση αποτυχίας
        return [("Property Type", "Host ID", "Property Count")]

    finally:
        cursor.close()  # Κλείσιμο του cursor
        connection.close()  # Κλείσιμο της σύνδεσης με τη βάση δεδομένων
     
def findMatchingProperties(guest_id):
    connection = pymysql.connect(**db_config)
    cursor = connection.cursor()
    
    ### YOUR CODE HERE
    # Βήμα 1: Ανάκτηση καταλυμάτων με κοινές παροχές και κανόνες
    try:
            # Βήμα 1: Ανάκτηση καταλυμάτων με κοινές παροχές και κανόνες
            main_query = """
                         SELECT DISTINCT b.guest_id, p1.property_id
                         FROM property_has_amenity p1, \
                              wishlist w, \
                              wishlist_has_property wp, \
                              property_has_amenity p2, \
                              property_has_rule r1, \
                              booking b, \
                              property_has_rule r2
                         WHERE w.guest_id = %s
                           AND w.wishlist_id = wp.wishlist_id
                           AND wp.property_id = p2.property_id
                           AND p1.amenity_id = p2.amenity_id
                           AND b.guest_id = %s
                           AND b.property_id = r2.property_id
                           AND r1.rule_id = r2.rule_id
                           AND p1.property_id = r1.property_id; \
                         """

            cursor.execute(main_query, (guest_id, guest_id))
            results = cursor.fetchall()

            # Βήμα 2: Ανάκτηση των αποκλεισμένων οικοδεσποτών
            blocked_hosts_query = """
                                  SELECT DISTINCT p.host_id
                                  FROM property p, \
                                       booking b
                                  WHERE b.guest_id = %s
                                    AND b.property_id = p.property_id; \
                                  """

            cursor.execute(blocked_hosts_query, (guest_id,))
            blocked_hosts = set([host[0] for host in
                                 cursor.fetchall()])  # Αποθηκεύουμε τους αποκλεισμένους οικοδεσπότες σε set για γρήγορο έλεγχο

            # Βήμα 3: Συλλογή των τελικών αποτελεσμάτων με το σωστό format
            final_results = [("Property ID", "Property Name", "Amenities", "House Rules")]

            # Ανάκτηση όλων των καταλυμάτων (χωρίς φιλτράρισμα host)
            cursor.execute("SELECT property_id, name, host_id FROM property")
            all_properties = cursor.fetchall()

            # Δημιουργία λεξικού για γρήγορο έλεγχο host_id
            property_host_map = {prop[0]: prop[2] for prop in all_properties}
            property_name_map = {prop[0]: prop[1] for prop in all_properties}

            # Φιλτράρουμε τα αποτελέσματα σε Python
            for guest, property_id in results:
                host_id = property_host_map.get(property_id)

                # Αν ο host_id ΔΕΝ είναι στους αποκλεισμένους, το κρατάμε
                if host_id not in blocked_hosts:
                    # Παροχές (Amenities)
                    cursor.execute(
                        "SELECT a.amenity_name FROM property_has_amenity pa, amenity a WHERE pa.property_id = %s AND pa.amenity_id = a.amenity_id;",
                        (property_id,))
                    amenities = [row[0] for row in cursor.fetchall()]

                    # Κανόνες Διαμονής (House Rules)
                    cursor.execute(
                        "SELECT hr.rule_name FROM property_has_rule pr, houserule hr WHERE pr.property_id = %s AND pr.rule_id = hr.rule_id;",
                        (property_id,))
                    house_rules = [row[0] for row in cursor.fetchall()]

                    # Προσθήκη του καταλύματος στα τελικά αποτελέσματα
                    final_results.append(
                        (property_id, property_name_map[property_id], ", ".join(amenities), ", ".join(house_rules)))

            return final_results

    except Exception as e:
            print(f"Σφάλμα κατά την εκτέλεση του query: {e}")
            return [("Error",)]

    finally:
            # Κλείσιμο της σύνδεσης
            cursor.close()
            connection.close()

def countWordsForProperties(N, M):
    connection = pymysql.connect(**db_config)
    cursor = connection.cursor()

    try:
        # Λίστα με stopwords για να αγνοηθούν στις αναλύσεις κειμένου
        stopwords = set([
            "and", "or", "not", "to", "at", "a", "the", "is", "in", "on", "of", "for", "with",
            "by", "this", "that", "from", "it", "as", "an", "be", "are", "was", "were", "has", "have"
        ])

        import re

        # Βήμα 1: Φτιάχνουμε λεξικό property_id -> set(guest_id) από κρατήσεις
        cursor.execute("SELECT property_id, guest_id FROM booking")
        bookings = cursor.fetchall()

        guests_per_property = {}
        for prop_id, guest_id in bookings:
            guests_per_property.setdefault(prop_id, set()).add(guest_id)

        # Φιλτράρουμε ακίνητα με >= N διαφορετικούς επισκέπτες
        filtered_props = [pid for pid, guests in guests_per_property.items() if len(guests) >= int(N)]

        # Βήμα 2: Από αυτά κρατάμε μόνο όσα έχουν τουλάχιστον 1 review
        props_with_review = []
        for pid in filtered_props:
            cursor.execute("SELECT review_id FROM review WHERE property_id=%s", (pid,))
            if cursor.fetchone() is not None:
                props_with_review.append(pid)

        # Βήμα 3: Φιλτράρουμε ακίνητα που έχουν τουλάχιστον 2 amenities
        props_with_amenities = []
        for pid in props_with_review:
            cursor.execute("SELECT amenity_id FROM property_has_amenity WHERE property_id=%s", (pid,))
            amenities = cursor.fetchall()
            if len(amenities) >= 2:
                props_with_amenities.append(pid)

        # Βήμα 4: Φιλτράρουμε ακίνητα που δεν είναι σε wishlist
        final_props = []
        for pid in props_with_amenities:
            cursor.execute("SELECT property_id FROM wishlist_has_property WHERE property_id=%s", (pid,))
            if cursor.fetchone() is None:
                final_props.append(pid)

        result = []

        # result.append((message,))

        # Για κάθε ακίνητο που πέρασε τα φίλτρα, συγκεντρώνουμε στοιχεία
        for pid in final_props:
            # Όνομα και τοποθεσία
            cursor.execute("SELECT name, location FROM property WHERE property_id=%s", (pid,))
            row = cursor.fetchone()
            name, location = row if row else ("", "")

            # Αριθμός μοναδικών επισκεπτών
            unique_guests = len(guests_per_property.get(pid, []))

            # Παροχές (amenities)
            cursor.execute("SELECT amenity_id FROM property_has_amenity WHERE property_id=%s", (pid,))
            amenity_rows = cursor.fetchall()
            amenities_list = []
            for (amenity_id,) in amenity_rows:
                cursor.execute("SELECT amenity_name FROM amenity WHERE amenity_id=%s", (amenity_id,))
                amenity_name = cursor.fetchone()
                if amenity_name:
                    amenities_list.append(amenity_name[0])
            amenities_str = ", ".join(amenities_list)

            # Συλλογή κειμένων από τις κριτικές
            cursor.execute("SELECT comment FROM review WHERE property_id=%s", (pid,))
            comments = cursor.fetchall()
            all_text = " ".join([c[0].lower() if c[0] else "" for c in comments])

            # Επεξεργασία κειμένου: αφαίρεση stopwords και υπολογισμός συχνότητας λέξεων
            words = re.findall(r'\b\w+\b', all_text)
            filtered_words = [w for w in words if w not in stopwords]

            freq = {}
            for w in filtered_words:
                freq[w] = freq.get(w, 0) + 1

            # Παίρνουμε τις top M πιο συχνές λέξεις
            top_words = sorted(freq.items(), key=lambda x: x[1], reverse=True)[:int(M)]
            top_words_str = ", ".join([w for w, count in top_words])

            # Προσθέτουμε τη γραμμή στο αποτέλεσμα
            result.append((pid, name, location, unique_guests, amenities_str, top_words_str))

    except Exception as e:
        # Σε περίπτωση λάθους επιστρέφουμε το μήνυμα λάθους
        # result = [("Error", str(e))]
        print(f"[countWordsForProperties] Error: {e}")
        result = []  # άδειος πίνακας
    # return [("Property ID","Name","Location","Unique guests","Amenities","Top Words")] + result
     

    finally:
        cursor.close()
        connection.close()

    # Επιστρέφουμε τον πίνακα με header + αποτελέσματα
    return [("Property ID", "Name", "Location", "Unique guests", "Amenities", "Top Words")] + result


def findCommonPropertiesAndGuests(guest_id_a, guest_id_b):
    HEADER = [("Property Name", "Guest C", "Guest D", "Guest A", "Guest B")]
    try:
        connection = pymysql.connect(**db_config)
        cursor = connection.cursor()
        
        # Πάρε ακίνητα που έχει μείνει ο a
        cursor.execute("SELECT property_id FROM booking WHERE guest_id = %s", (guest_id_a,))
        properties_a = set(row[0] for row in cursor.fetchall())
        
        # Πάρε ακίνητα που έχει μείνει ο b
        cursor.execute("SELECT property_id FROM booking WHERE guest_id = %s", (guest_id_b,))
        properties_b = set(row[0] for row in cursor.fetchall())
        
        if not properties_a or not properties_b:
            return HEADER
        
        # Βρες επισκέπτες που έχουν μείνει σε ακίνητα a
        guests_c_list = []
        for prop in properties_a:
            cursor.execute("SELECT guest_id FROM booking WHERE property_id = %s", (prop,))
            guests_c_list.extend([row[0] for row in cursor.fetchall()])
        guests_c = set(guests_c_list)
        
        # Βρες επισκέπτες που έχουν μείνει σε ακίνητα b
        guests_d_list = []
        for prop in properties_b:
            cursor.execute("SELECT guest_id FROM booking WHERE property_id = %s", (prop,))
            guests_d_list.extend([row[0] for row in cursor.fetchall()])
        guests_d = set(guests_d_list)
        
        # Αφαιρούμε a και b
        guests_c.discard(guest_id_a)
        guests_c.discard(guest_id_b)
        guests_d.discard(guest_id_a)
        guests_d.discard(guest_id_b)
        
        results = []
        checked_pairs = set()  # για αποφυγή διπλοτύπων
        
        for c in guests_c:
            cursor.execute("SELECT property_id FROM booking WHERE guest_id = %s", (c,))
            c_props = set(row[0] for row in cursor.fetchall())
            if len(c_props.intersection(properties_a)) == 0:
                continue
            
            for d in guests_d:
                if c == d:
                    continue
                
                pair = tuple(sorted((c, d)))
                if pair in checked_pairs:
                    continue
                checked_pairs.add(pair)

                cursor.execute("SELECT property_id FROM booking WHERE guest_id = %s", (d,))
                d_props = set(row[0] for row in cursor.fetchall())
                if len(d_props.intersection(properties_b)) == 0:
                    continue
                
                common_props = c_props.intersection(d_props)
                if not common_props:
                    continue
                
                for prop_id in common_props:
                    cursor.execute("SELECT name FROM property WHERE property_id = %s", (prop_id,))
                    title = cursor.fetchone()
                    if title:
                        results.append((title[0], c, d, guest_id_a, guest_id_b))
        
        if not results:
            return HEADER
        
        return HEADER + results
    
    except Exception as e:
        print(f"Error in findCommonPropertiesAndGuests: {e}")
        return HEADER 
    finally:
        try:
            cursor.close()
            connection.close()
        except:
            pass

from decimal import Decimal

def highValueHost(min_price_booking, min_rating_review, min_avg_price_host, min_avg_rating_host):
    try:
        min_price_booking = Decimal(min_price_booking)
        min_rating_review = Decimal(min_rating_review)
        min_avg_price_host = Decimal(min_avg_price_host)
        min_avg_rating_host = Decimal(min_avg_rating_host)
    except Exception as e:
        print("Invalid input parameters:", e)
        return [("Amenity", "Frequency",),]

    connection = pymysql.connect(**db_config)
    cursor = connection.cursor()
    try:
        cursor.execute("SELECT property_id, host_id, price, rating FROM property")
        properties = cursor.fetchall()

        host_prices = {}
        host_ratings = {}
        host_counts = {}

        for prop_id, host_id, price, rating in properties:
            if price is None or rating is None:
                continue
            host_prices[host_id] = host_prices.get(host_id, 0) + price
            host_ratings[host_id] = host_ratings.get(host_id, 0) + rating
            host_counts[host_id] = host_counts.get(host_id, 0) + 1

        high_value_hosts = set()
        for host_id in host_counts:
            avg_price = host_prices[host_id] / host_counts[host_id]
            avg_rating = host_ratings[host_id] / host_counts[host_id]
            if avg_price >= min_avg_price_host and avg_rating >= min_avg_rating_host:
                high_value_hosts.add(host_id)

        if not high_value_hosts:
            return [("Amenity", "Frequency",),]

        cursor.execute("SELECT guest_id, property_id FROM booking")
        bookings = cursor.fetchall()

        cursor.execute("SELECT guest_id, rating FROM review")
        reviews = cursor.fetchall()

        property_prices = {prop_id: price for (prop_id, host_id, price, rating) in properties}

        guests_price_ok = set()
        for guest_id, prop_id in bookings:
            price = property_prices.get(prop_id)
            if price is not None and price >= min_price_booking:
                guests_price_ok.add(guest_id)

        if not guests_price_ok:
            return [("Amenity", "Frequency",),]

        guests_rating_ok = set()
        for guest_id, rating in reviews:
            if rating is not None and rating >= min_rating_review:
                guests_rating_ok.add(guest_id)

        if not guests_rating_ok:
            return [("Amenity", "Frequency",),]

        high_value_guests = guests_price_ok.intersection(guests_rating_ok)

        if not high_value_guests:
            return [("Amenity", "Frequency",),]

        properties_of_high_value_hosts = set()
        for prop_id, host_id, price, rating in properties:
            if host_id in high_value_hosts:
                properties_of_high_value_hosts.add(prop_id)

        if not properties_of_high_value_hosts:
            return [("Amenity", "Frequency",),]

        properties_booked_by_guests = set()
        for guest_id, prop_id in bookings:
            if guest_id in high_value_guests:
                properties_booked_by_guests.add(prop_id)

        if not properties_booked_by_guests:
            return [("Amenity", "Frequency",),]

        valid_properties = properties_of_high_value_hosts.intersection(properties_booked_by_guests)

        if not valid_properties:
            return [("Amenity", "Frequency",),]

        cursor.execute("SELECT amenity_id, amenity_name FROM amenity")
        amenities = cursor.fetchall()

        cursor.execute("SELECT property_id, amenity_id FROM property_has_amenity")
        property_amenities = cursor.fetchall()

        amenity_counts = {}
        amenity_id_to_name = {aid: aname for (aid, aname) in amenities}

        for prop_id, amenity_id in property_amenities:
            if prop_id in valid_properties:
                amenity_name = amenity_id_to_name.get(amenity_id)
                if amenity_name:
                    amenity_counts[amenity_name] = amenity_counts.get(amenity_name, 0) + 1

        result = sorted(amenity_counts.items(), key=lambda x: x[1], reverse=True)

        return [("Amenity", "Frequency",),] + result

    except Exception as e:
        print("Error:", e)
        return [("Amenity", "Frequency",), ("nothing found", 0)]

    finally:
        cursor.close()
        connection.close()
        

def recommendProperty(guest_id, desired_city, desired_amenities, max_price, min_rating):
    import json
    HEADER = [("Property ID", "Property Name")]

    if isinstance(desired_amenities, str):
        try:
            desired_amenities = json.loads(desired_amenities)
        except:
            return HEADER + [("ERROR", "Invalid amenities format",)]

    connection = pymysql.connect(**db_config)
    cursor = connection.cursor()

    try:
        # 1. Βρες όλα τα properties με φίλτρο location, price, rating
        sql_properties = """
            SELECT property_id, name, rating, price
            FROM property
            WHERE location = %s AND price <= %s AND rating >= %s
        """
        cursor.execute(sql_properties, (desired_city, max_price, min_rating))
        properties = cursor.fetchall()  # πχ [(1, "Prop1", 4.5, 80.0), (2, "Prop2", 4.8, 90.0), ...]

        if not properties:
            return HEADER + [("", "No suitable properties found",)]

        property_ids = [p[0] for p in properties]

        # 2. Πάρε όλα τα amenity_id και amenity_name για όλα τα properties μαζεμένα (όχι ανά property)
        # Μπορούμε να κάνουμε ένα query με WHERE property_id IN (...) χωρίς subselect, γιατί δεν είναι εμφωλευμένο SELECT
        format_strings = ",".join(["%s"] * len(property_ids))
        sql_amenities = f"""
            SELECT pha.property_id, a.amenity_name
            FROM property_has_amenity pha
            JOIN amenity a ON pha.amenity_id = a.amenity_id
            WHERE pha.property_id IN ({format_strings})
        """
        cursor.execute(sql_amenities, tuple(property_ids))
        rows = cursor.fetchall()  # [(property_id, amenity_name), ...]

        property_amenities = {}
        for prop_id in property_ids:
            property_amenities[prop_id] = set()
        for prop_id, amenity_name in rows:
            property_amenities[prop_id].add(amenity_name)

        # 3. Υπολόγισε weighted score για κάθε property
        max_weight_sum = sum(desired_amenities.values())
        scored_properties = []

        for prop_id, name, rating, price in properties:
            amenities_score = 0
            for amenity, weight in desired_amenities.items():
                if amenity in property_amenities.get(prop_id, set()):
                    amenities_score += weight
            amenity_score_norm = amenities_score / max_weight_sum if max_weight_sum > 0 else 0

            w_rating = 0.7
            w_amenities = 0.3
            combined_score = (float(rating) * w_rating) + (amenity_score_norm * w_amenities * 5)
            scored_properties.append((prop_id, name, combined_score))

        scored_properties.sort(key=lambda x: x[2], reverse=True)
        best_property = scored_properties[0] if scored_properties else None

        if not best_property:
            return HEADER + [("", "No suitable properties found",)]

        best_property_id, best_property_name = best_property[0], best_property[1]

        # 4. Δημιουργία wishlist και εισαγωγή στη βάση
        wishlist_name = f"Recommended for guest {guest_id}"
        privacy = "Private"

        sql_insert_wishlist = "INSERT INTO wishlist (guest_id, name, privacy) VALUES (%s, %s, %s)"
        cursor.execute(sql_insert_wishlist, (guest_id, wishlist_name, privacy))
        wishlist_id = cursor.lastrowid

        sql_insert_wishlist_prop = "INSERT INTO wishlist_has_property (wishlist_id, property_id) VALUES (%s, %s)"
        cursor.execute(sql_insert_wishlist_prop, (wishlist_id, best_property_id))

        connection.commit()

        return HEADER + [(best_property_id, best_property_name)]

    except Exception as e:
        connection.rollback()
        return HEADER + [("ERROR", str(e))]
    finally:
        cursor.close()
        connection.close()

