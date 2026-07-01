"""
Seed database with realistic movie trend data for demo/portfolio purposes.

Usage: python manage.py seed_data
"""
import random
from datetime import date, timedelta

from django.core.management.base import BaseCommand

from trends.models import (
    DailyTrend,
    Genre,
    MonthlyGenreStat,
    Movie,
    MovieCast,
    MovieGenre,
    Person,
    WeeklySummary,
)

# Real TMDB genre IDs
GENRES = [
    (28, "Action"), (12, "Adventure"), (16, "Animation"), (35, "Comedy"),
    (80, "Crime"), (99, "Documentary"), (18, "Drama"), (10751, "Family"),
    (14, "Fantasy"), (36, "History"), (27, "Horror"), (10402, "Music"),
    (9648, "Mystery"), (10749, "Romance"), (878, "Science Fiction"),
    (10770, "TV Movie"), (53, "Thriller"), (10752, "War"), (37, "Western"),
]

# Real movie data with actual TMDB poster paths
MOVIES = [
    {"id": 27205, "title": "Inception", "budget": 160000000, "revenue": 836800000, "release_date": "2010-07-16", "runtime": 148, "lang": "en", "poster": "/edv5CZvWj09upOsy2Y6IwDhK8bt.jpg", "backdrop": "/s3TBrRGB1iav7gFOCNx3H31MoES.jpg", "genres": [28, 878, 12]},
    {"id": 155, "title": "The Dark Knight", "budget": 185000000, "revenue": 1004558444, "release_date": "2008-07-18", "runtime": 152, "lang": "en", "poster": "/qJ2tW6WMUDux911r6m7haRef0WH.jpg", "backdrop": "/nMKdUUepR0i5zn0y1T4CsSB5ez9.jpg", "genres": [28, 80, 18]},
    {"id": 872585, "title": "Oppenheimer", "budget": 100000000, "revenue": 952000000, "release_date": "2023-07-21", "runtime": 181, "lang": "en", "poster": "/8Gxv8gSFCU0XGDykEGv7zR1n2ua.jpg", "backdrop": "/fm6KqXpk3M2HVveHwCrBSSBaO0V.jpg", "genres": [18, 36]},
    {"id": 346698, "title": "Barbie", "budget": 145000000, "revenue": 1441867467, "release_date": "2023-07-21", "runtime": 114, "lang": "en", "poster": "/iuFNMS8U5cb6xfzi51Dbkovj7vM.jpg", "backdrop": "/nHf61UzkfFno5X1ofIhugCPus2R.jpg", "genres": [35, 12, 14]},
    {"id": 693134, "title": "Dune: Part Two", "budget": 190000000, "revenue": 714444358, "release_date": "2024-02-27", "runtime": 166, "lang": "en", "poster": "/1pdfLvkbY9ohJlCjQH2CZjjYVvJ.jpg", "backdrop": "/xOMo8BRK7PfcJv9JCnx7s5hj0PX.jpg", "genres": [878, 12]},
    {"id": 438631, "title": "Dune", "budget": 165000000, "revenue": 407577856, "release_date": "2021-09-15", "runtime": 155, "lang": "en", "poster": "/d5NXSklXo0qyIYkgV94XAgMIckC.jpg", "backdrop": "/jYEW5xZkZk2WTrdbMGAPFuBqbDc.jpg", "genres": [878, 12]},
    {"id": 634649, "title": "Spider-Man: No Way Home", "budget": 200000000, "revenue": 1921847111, "release_date": "2021-12-15", "runtime": 148, "lang": "en", "poster": "/1g0dhYtq4irTY1GPXvft6k4YLjm.jpg", "backdrop": "/14QbnygCuTO0vl7CAFmPf1fgZfV.jpg", "genres": [28, 12, 878]},
    {"id": 76600, "title": "Avatar: The Way of Water", "budget": 350000000, "revenue": 2320250281, "release_date": "2022-12-14", "runtime": 192, "lang": "en", "poster": "/t6HIqrRAclMCA60NsSmeqe9RmNV.jpg", "backdrop": "/s16H6tpK2utvwDtzZ8Qy4qm5Emw.jpg", "genres": [878, 12, 28]},
    {"id": 786892, "title": "Furiosa: A Mad Max Saga", "budget": 168000000, "revenue": 173000000, "release_date": "2024-05-22", "runtime": 149, "lang": "en", "poster": "/iADOJ8Zymht2JPMoy3R7xceZprc.jpg", "backdrop": "/wN48whFM19b1Y0pxyNLj8oW3V47.jpg", "genres": [28, 12, 878]},
    {"id": 653346, "title": "Kingdom of the Planet of the Apes", "budget": 160000000, "revenue": 397373480, "release_date": "2024-05-08", "runtime": 145, "lang": "en", "poster": "/gKkl37BQuKTanygYQG1pyYgLVgf.jpg", "backdrop": "/fqv8v6AycXKsivp1T5yKtLbGXce.jpg", "genres": [878, 28, 12]},
    {"id": 1022789, "title": "Inside Out 2", "budget": 200000000, "revenue": 1698500000, "release_date": "2024-06-11", "runtime": 100, "lang": "en", "poster": "/vpnVM9B6NMmQpWeZvzLvDESb2QY.jpg", "backdrop": "/xg27NrXi7VXCGUr7MN75UmLBOEs.jpg", "genres": [16, 10751, 35]},
    {"id": 533535, "title": "Deadpool & Wolverine", "budget": 200000000, "revenue": 1338073645, "release_date": "2024-07-24", "runtime": 128, "lang": "en", "poster": "/8cdWjvZQUExUUTzyp4t6EDMubfO.jpg", "backdrop": "/yDHYTfA3R0jFYba16jBB1ef8oIt.jpg", "genres": [28, 35, 878]},
    {"id": 1184918, "title": "The Wild Robot", "budget": 78000000, "revenue": 325000000, "release_date": "2024-09-12", "runtime": 102, "lang": "en", "poster": "/wTnV3PCVW5O92JMrFvvrRcV39RU.jpg", "backdrop": "/4zlOPT9CrtIzs0f3IKrHGjK58dh.jpg", "genres": [16, 878, 10751]},
    {"id": 912649, "title": "Venom: The Last Dance", "budget": 120000000, "revenue": 478000000, "release_date": "2024-10-22", "runtime": 109, "lang": "en", "poster": "/aosm8NMQ3UyoBVpSxyimorCQykC.jpg", "backdrop": "/3V4kLQg0kSqPLctI5ziYWabAZYF.jpg", "genres": [28, 878, 12]},
    {"id": 1399, "title": "Game of Thrones", "budget": 0, "revenue": 0, "release_date": "2011-04-17", "runtime": 60, "lang": "en", "poster": "/1XS1oqL89opfnbLl8WnZY1O1uJx.jpg", "backdrop": "/suopoADq0k8YZr4dQXcU6pToj6s.jpg", "genres": [10765, 18, 10759], "media_type": "tv"},
    {"id": 1396, "title": "Breaking Bad", "budget": 0, "revenue": 0, "release_date": "2008-01-20", "runtime": 45, "lang": "en", "poster": "/3xnWaLQjelJDDF7LT1WBo6f4BRe.jpg", "backdrop": "/900tHlUYUkp7Ol04XFSoGzBeVyV.jpg", "genres": [18, 80], "media_type": "tv"},
    {"id": 698687, "title": "Transformers One", "budget": 75000000, "revenue": 172000000, "release_date": "2024-09-11", "runtime": 104, "lang": "en", "poster": "/iRCgqpdVE4wyLQvGYU3ZP7pAtUc.jpg", "backdrop": "/x9JnGFwXluJsA85IgPCJzl8URcm.jpg", "genres": [16, 28, 878]},
    {"id": 545611, "title": "Everything Everywhere All at Once", "budget": 25000000, "revenue": 141000000, "release_date": "2022-03-11", "runtime": 139, "lang": "en", "poster": "/w3LxiVYdWWRvEVdn5RYq6jIqkb1.jpg", "backdrop": "/fOy2Jurz9k6RnJnMUMRDAgBwru2.jpg", "genres": [28, 12, 878]},
    {"id": 603692, "title": "John Wick: Chapter 4", "budget": 100000000, "revenue": 440157033, "release_date": "2023-03-22", "runtime": 169, "lang": "en", "poster": "/vZloFAK7NmvMGKE7VkF5UHaz0I.jpg", "backdrop": "/7I6VUdPj6tQECNHdviJkUHD2u89.jpg", "genres": [28, 53, 80]},
    {"id": 569094, "title": "Spider-Man: Across the Spider-Verse", "budget": 100000000, "revenue": 690700000, "release_date": "2023-05-31", "runtime": 140, "lang": "en", "poster": "/8Vt6mWEReuy4Of61Lnj5Xj704m8.jpg", "backdrop": "/4HodYYKEIsGOdinkGi2Ucz6X9i0.jpg", "genres": [16, 28, 12]},
    {"id": 447365, "title": "Guardians of the Galaxy Vol. 3", "budget": 250000000, "revenue": 845600000, "release_date": "2023-05-03", "runtime": 150, "lang": "en", "poster": "/r2J02Z2OpNTctfOSN1Ydgii51I3.jpg", "backdrop": "/lzWHmYdfeFiMIY4JaMmtR7GEli3.jpg", "genres": [878, 12, 35]},
    {"id": 385687, "title": "Fast X", "budget": 340000000, "revenue": 714752672, "release_date": "2023-05-17", "runtime": 141, "lang": "en", "poster": "/fiVW06jE7z9YnO4trhaMEdclSiC.jpg", "backdrop": "/4XM8DUTQb3lhLemJC51Jx4a2EuA.jpg", "genres": [28, 80, 53]},
    {"id": 640146, "title": "Ant-Man and the Wasp: Quantumania", "budget": 200000000, "revenue": 476070688, "release_date": "2023-02-15", "runtime": 124, "lang": "en", "poster": "/ngl2FKBlU4fhbdsrtdom9LVLBXw.jpg", "backdrop": "/3CxUndGhUcZdt1Zggjdb2HkLLQX.jpg", "genres": [28, 12, 878]},
    {"id": 502356, "title": "The Super Mario Bros. Movie", "budget": 100000000, "revenue": 1361000000, "release_date": "2023-04-05", "runtime": 92, "lang": "en", "poster": "/qNBAXBIQlnOThrVvA6mA2B5ggV6.jpg", "backdrop": "/9n2tJBplPbgR2ca05hS5CKXwP2c.jpg", "genres": [16, 12, 10751]},
    {"id": 823464, "title": "Godzilla x Kong: The New Empire", "budget": 135000000, "revenue": 571800000, "release_date": "2024-03-27", "runtime": 115, "lang": "en", "poster": "/z1p34vh7dEOnLDmyCrlUVLuoDzd.jpg", "backdrop": "/xRd1eJIDe7JHO5u4gtEYwGn5wtf.jpg", "genres": [28, 878, 12]},
    {"id": 438148, "title": "Minions: The Rise of Gru", "budget": 80000000, "revenue": 939628000, "release_date": "2022-06-29", "runtime": 87, "lang": "en", "poster": "/wKiOkZTN9lUUUNZLmtnwubZYONg.jpg", "backdrop": "/9s7fBt3kLSRTTX4Ylr2gQtuEcNN.jpg", "genres": [16, 35, 10751]},
    {"id": 1011985, "title": "Kung Fu Panda 4", "budget": 85000000, "revenue": 545000000, "release_date": "2024-03-02", "runtime": 94, "lang": "en", "poster": "/kDp1vUBnMpe8ak4rjgl3cLELqjU.jpg", "backdrop": "/kYgQzzjNis5jJalYtIHgrom0gOx.jpg", "genres": [16, 28, 35]},
]

PEOPLE = [
    {"id": 6193, "name": "Leonardo DiCaprio", "dept": "Acting", "popularity": 65.3, "profile": "/wo2hJpn04vbtmh0B9utCFdsQhxM.jpg"},
    {"id": 17419, "name": "Bryan Cranston", "dept": "Acting", "popularity": 28.5, "profile": "/7Jahy5LZX2Fo8fGJltMreAI49hc.jpg"},
    {"id": 2524, "name": "Tom Hardy", "dept": "Acting", "popularity": 52.1, "profile": "/d81K0RH8UX7tZj49tZaQhZ9ewH.jpg"},
    {"id": 1136406, "name": "Tom Holland", "dept": "Acting", "popularity": 72.4, "profile": "/bBRlrpJm9XkNSg0YT5LCaxqoFMX.jpg"},
    {"id": 2888, "name": "Will Smith", "dept": "Acting", "popularity": 38.7, "profile": "/7emRbuYL1VT4SnHqLKjXQEXajv2.jpg"},
    {"id": 1245, "name": "Scarlett Johansson", "dept": "Acting", "popularity": 55.2, "profile": "/6NsMbJXRlDZuDzatN2akFdGuTvx.jpg"},
    {"id": 17604, "name": "Zendaya", "dept": "Acting", "popularity": 68.9, "profile": "/6TE2AlOUqcrs7CyJiWYgodmee1r.jpg"},
    {"id": 5292, "name": "Denzel Washington", "dept": "Acting", "popularity": 31.4, "profile": "/khMbc5vmOMS6cR8aIW4ICwmg1Fk.jpg"},
    {"id": 1190668, "name": "Timothée Chalamet", "dept": "Acting", "popularity": 71.8, "profile": "/BE2sdjpgsa2rNTFa66f7upkaOP.jpg"},
    {"id": 18897, "name": "Jackie Chan", "dept": "Acting", "popularity": 25.3, "profile": "/nraZoTzlJBPXbVtB59ORHApGMoP.jpg"},
    {"id": 73457, "name": "Chris Pratt", "dept": "Acting", "popularity": 47.6, "profile": "/83o3koL82jt30EJ0rz4Bnzrt2dd.jpg"},
    {"id": 17647, "name": "Margot Robbie", "dept": "Acting", "popularity": 58.1, "profile": "/euDPyqLnuwaWMHR6JN0YNRE1Qj5.jpg"},
    {"id": 880, "name": "Ben Affleck", "dept": "Acting", "popularity": 22.7, "profile": "/aSMEzMOMCjOp5HMrmCVpfBlr3yR.jpg"},
    {"id": 1397778, "name": "Anya Taylor-Joy", "dept": "Acting", "popularity": 48.3, "profile": "/jxAbDJWvz4p3EvhCLBMbEPsOqq8.jpg"},
    {"id": 10859, "name": "Ryan Reynolds", "dept": "Acting", "popularity": 63.5, "profile": "/algQ1VEno2W9SesoArWcZxnmMkA.jpg"},
]


class Command(BaseCommand):
    help = "Seed database with realistic movie trend data for demo"

    def handle(self, *args, **options):
        self.stdout.write(" Seeding CineTrends database...")

        # Genres
        for gid, name in GENRES:
            Genre.objects.update_or_create(genre_id=gid, defaults={"name": name})
        self.stdout.write(f"   {len(GENRES)} genres")

        # Movies
        for m in MOVIES:
            movie, _ = Movie.objects.update_or_create(
                movie_id=m["id"],
                defaults={
                    "title": m["title"],
                    "budget": m["budget"],
                    "revenue": m["revenue"],
                    "release_date": m["release_date"],
                    "runtime": m["runtime"],
                    "original_language": m["lang"],
                    "poster_url": f"https://image.tmdb.org/t/p/w500{m['poster']}",
                    "backdrop_url": f"https://image.tmdb.org/t/p/w1280{m['backdrop']}",
                    "roi": (m["revenue"] - m["budget"]) / m["budget"] * 100 if m["budget"] else None,
                    "overview": f"A critically acclaimed {m['title']} that captivated audiences worldwide.",
                },
            )
            # Link genres
            for gid in m.get("genres", []):
                MovieGenre.objects.get_or_create(movie_id=m["id"], genre_id=gid)
        self.stdout.write(f"   {len(MOVIES)} movies")

        # People
        for p in PEOPLE:
            Person.objects.update_or_create(
                person_id=p["id"],
                defaults={
                    "name": p["name"],
                    "known_for_department": p["dept"],
                    "popularity": p["popularity"],
                    "profile_url": f"https://image.tmdb.org/t/p/w185{p['profile']}",
                },
            )
        self.stdout.write(f"   {len(PEOPLE)} people")

        # Cast links (random but plausible)
        cast_pairs = [
            (27205, 6193, "Dom Cobb"), (155, 6193, "N/A"),
            (693134, 1190668, "Paul Atreides"), (693134, 17604, "Chani"),
            (438631, 1190668, "Paul Atreides"), (438631, 17604, "Chani"),
            (346698, 17647, "Barbie"), (346698, 10859, "Ken"),
            (634649, 1136406, "Spider-Man"), (634649, 17604, "MJ"),
            (533535, 10859, "Deadpool"), (533535, 2524, "Wolverine"),
            (569094, 1136406, "Miles Morales"),
            (786892, 1397778, "Furiosa"),
            (1022789, 1245, "Joy"),
        ]
        for mid, pid, char in cast_pairs:
            MovieCast.objects.get_or_create(
                movie_id=mid, person_id=pid,
                defaults={"character_name": char, "cast_order": 0},
            )
        self.stdout.write(f"   {len(cast_pairs)} cast links")

        # Daily Trends (30 days of data)
        today = date.today()
        DailyTrend.objects.all().delete()
        trend_count = 0

        for day_offset in range(30):
            trend_date = today - timedelta(days=day_offset)
            # Shuffle movies to create dynamic rankings
            shuffled = list(MOVIES)
            random.shuffle(shuffled)

            for pos, m in enumerate(shuffled[:20], 1):
                prev_pos = pos + random.randint(-3, 3)
                DailyTrend.objects.create(
                    movie_id=m["id"],
                    media_type=m.get("media_type", "movie"),
                    date=trend_date,
                    position=pos,
                    position_change=prev_pos - pos,
                    popularity=random.uniform(50, 200) + (21 - pos) * 5,
                    vote_average=round(random.uniform(5.5, 9.2), 1),
                    vote_count=random.randint(500, 15000),
                )
                trend_count += 1

        self.stdout.write(f"   {trend_count} daily trend records (30 days)")

        # Weekly Summaries (4 weeks)
        WeeklySummary.objects.all().delete()
        weekly_count = 0
        for week in range(4):
            week_start = today - timedelta(days=today.weekday() + 7 * week)
            for m in random.sample(MOVIES, 15):
                WeeklySummary.objects.create(
                    movie_id=m["id"],
                    media_type=m.get("media_type", "movie"),
                    week_start=week_start,
                    days_trending=random.randint(1, 7),
                    avg_position=round(random.uniform(1, 20), 1),
                    best_position=random.randint(1, 10),
                    avg_popularity=round(random.uniform(60, 180), 1),
                    popularity_change_pct=round(random.uniform(-30, 50), 1),
                )
                weekly_count += 1
        self.stdout.write(f"   {weekly_count} weekly summaries")

        # Monthly Genre Stats (2 months)
        MonthlyGenreStat.objects.all().delete()
        monthly_count = 0
        for month_offset in range(2):
            month = today.replace(day=1) - timedelta(days=30 * month_offset)
            month = month.replace(day=1)
            existing_gids = list(set(g for m in MOVIES for g in m.get("genres", [])))
            for gid in random.sample(existing_gids, min(12, len(existing_gids))):
                top_movie = random.choice([m for m in MOVIES if gid in m.get("genres", [])])
                total_budget = random.randint(200, 2000) * 1000000
                total_revenue = random.randint(100, 5000) * 1000000
                avg_roi = round((total_revenue - total_budget) / total_budget * 100, 1)

                MonthlyGenreStat.objects.create(
                    month=month,
                    genre_id=gid,
                    trending_count=random.randint(3, 25),
                    avg_popularity=round(random.uniform(50, 150), 1),
                    avg_vote_average=round(random.uniform(6.0, 8.5), 1),
                    top_movie_id=top_movie["id"],
                    total_budget=total_budget,
                    total_revenue=total_revenue,
                    avg_roi=avg_roi,
                )
                monthly_count += 1
        self.stdout.write(f"   {monthly_count} monthly genre stats")

        self.stdout.write(self.style.SUCCESS("\n Seeding complete."))
