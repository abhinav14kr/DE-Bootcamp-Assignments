{
 "cells": [
  {
   "cell_type": "code",
   "execution_count": 1,
   "id": "4c89446c-d475-4ae6-aa15-a38331b02a8f",
   "metadata": {},
   "outputs": [
    {
     "name": "stderr",
     "output_type": "stream",
     "text": [
      "25/01/31 02:54:38 WARN SparkSession: Using an existing Spark session; only runtime SQL configurations will take effect.\n"
     ]
    }
   ],
   "source": [
    "from pyspark. sql import SparkSession\n",
    "from pyspark.sql.functions import expr, col, lit\n",
    "spark = SparkSession.builder.appName(\"Jupyter\").getOrCreate()"
   ]
  },
  {
   "cell_type": "code",
   "execution_count": 2,
   "id": "a9a86d4b-4c9d-482b-9888-aa537fd6fbad",
   "metadata": {},
   "outputs": [
    {
     "name": "stderr",
     "output_type": "stream",
     "text": [
      "25/01/31 02:54:45 WARN SparkStringUtils: Truncated the string representation of a plan since it was too large. This behavior can be adjusted by setting 'spark.sql.debug.maxToStringFields'.\n"
     ]
    },
    {
     "data": {
      "text/plain": [
       "[Row(medal_id=2315448068, sprite_uri=None, sprite_left=None, sprite_top=None, sprite_sheet_width=None, sprite_sheet_height=None, sprite_width=None, sprite_height=None, classification=None, description=None, name=None, difficulty=None),\n",
       " Row(medal_id=3565441934, sprite_uri=None, sprite_left=None, sprite_top=None, sprite_sheet_width=None, sprite_sheet_height=None, sprite_width=None, sprite_height=None, classification=None, description=None, name=None, difficulty=None),\n",
       " Row(medal_id=4162659350, sprite_uri='https://content.halocdn.com/media/Default/games/halo-5-guardians/sprites/medalspritesheet-be288ea5c0994a4e9d36f43aee7bc631.png', sprite_left=750, sprite_top=750, sprite_sheet_width=74, sprite_sheet_height=74, sprite_width=1125, sprite_height=899, classification='Breakout', description='Kill the last enemy within the last 10 seconds of a round.', name='Buzzer Beater', difficulty=45),\n",
       " Row(medal_id=1573153198, sprite_uri='https://content.halocdn.com/media/Default/games/halo-5-guardians/sprites/medalspritesheet-be288ea5c0994a4e9d36f43aee7bc631.png', sprite_left=0, sprite_top=300, sprite_sheet_width=74, sprite_sheet_height=74, sprite_width=1125, sprite_height=899, classification='Breakout', description='Survive a one-on-one encounter.', name='Vanquisher', difficulty=30),\n",
       " Row(medal_id=298813630, sprite_uri='https://content.halocdn.com/media/Default/games/halo-5-guardians/sprites/medalspritesheet-be288ea5c0994a4e9d36f43aee7bc631.png', sprite_left=0, sprite_top=825, sprite_sheet_width=74, sprite_sheet_height=74, sprite_width=1125, sprite_height=899, classification='Style', description='Kill an enemy with Spartan Charge.', name='Spartan Charge', difficulty=135)]"
      ]
     },
     "execution_count": 2,
     "metadata": {},
     "output_type": "execute_result"
    }
   ],
   "source": [
    "maps_df = spark.read.option(\"header\", \"true\").option(\"inferSchema\", \"true\").csv(\"maps.csv\")\n",
    "matches_df = spark.read.option(\"header\", \"true\").option(\"inferSchema\", \"true\").csv(\"match_details.csv\")\n",
    "match_details_df = spark.read.option(\"header\", \"true\").option(\"inferSchema\", \"true\").csv(\"matches.csv\")\n",
    "medals_matches_players = spark.read.option(\"header\", \"true\").option(\"inferSchema\", \"true\").csv(\"medals_matches_players.csv\")\n",
    "medals = spark.read.option(\"header\", \"true\").option(\"inferSchema\", \"true\").csv(\"medals.csv\")\n",
    "matches_df.take(5)\n",
    "match_details_df.take(5)\n",
    "medals_matches_players.take(5)\n",
    "medals.take(5)"
   ]
  },
  {
   "cell_type": "code",
   "execution_count": 3,
   "id": "eae7db4f-20a6-41ec-b967-6bab3f02e4c7",
   "metadata": {},
   "outputs": [
    {
     "name": "stdout",
     "output_type": "stream",
     "text": [
      "-1\n"
     ]
    }
   ],
   "source": [
    "# Disabled automatic broadcast join with spark.conf.set(\"spark.sql.autoBroadcastJoinThreshold\", \"-1\")\n",
    "spark.conf.set(\"spark.sql.autoBroadcastJoinThreshold\", \"-1\")\n",
    "print(spark.conf.get(\"spark.sql.autoBroadcastJoinThreshold\"))"
   ]
  },
  {
   "cell_type": "code",
   "execution_count": 4,
   "id": "3c819f28-402d-424e-8700-56a0e9bf10d8",
   "metadata": {},
   "outputs": [
    {
     "name": "stdout",
     "output_type": "stream",
     "text": [
      "+----+--------+----------+-----------+----------+------------------+-------------------+------------+-------------+--------------+-----------+----------+-----+-----------+\n",
      "|name|medal_id|sprite_uri|sprite_left|sprite_top|sprite_sheet_width|sprite_sheet_height|sprite_width|sprite_height|classification|description|difficulty|mapid|description|\n",
      "+----+--------+----------+-----------+----------+------------------+-------------------+------------+-------------+--------------+-----------+----------+-----+-----------+\n",
      "+----+--------+----------+-----------+----------+------------------+-------------------+------------+-------------+--------------+-----------+----------+-----+-----------+\n",
      "\n"
     ]
    }
   ],
   "source": [
    "# Explicitly broadcast JOINs medals and maps\n",
    "from pyspark.sql.functions import broadcast\n",
    "result = medals.join(broadcast(maps_df), \"name\")\n",
    "result.show()"
   ]
  },
  {
   "cell_type": "code",
   "execution_count": 5,
   "id": "65313ff9-7374-4f16-8e9f-040b17cdf59c",
   "metadata": {},
   "outputs": [
    {
     "name": "stdout",
     "output_type": "stream",
     "text": [
      "['match_id', 'mapid', 'is_team_game', 'playlist_id', 'game_variant_id', 'is_match_over', 'completion_date', 'match_duration', 'game_mode', 'map_variant_id']\n",
      "['match_id', 'player_gamertag', 'previous_spartan_rank', 'spartan_rank', 'previous_total_xp', 'total_xp', 'previous_csr_tier', 'previous_csr_designation', 'previous_csr', 'previous_csr_percent_to_next_tier', 'previous_csr_rank', 'current_csr_tier', 'current_csr_designation', 'current_csr', 'current_csr_percent_to_next_tier', 'current_csr_rank', 'player_rank_on_team', 'player_finished', 'player_average_life', 'player_total_kills', 'player_total_headshots', 'player_total_weapon_damage', 'player_total_shots_landed', 'player_total_melee_kills', 'player_total_melee_damage', 'player_total_assassinations', 'player_total_ground_pound_kills', 'player_total_shoulder_bash_kills', 'player_total_grenade_damage', 'player_total_power_weapon_damage', 'player_total_power_weapon_grabs', 'player_total_deaths', 'player_total_assists', 'player_total_grenade_kills', 'did_win', 'team_id']\n",
      "['match_id', 'player_gamertag', 'medal_id', 'count']\n"
     ]
    }
   ],
   "source": [
    "print(match_details_df.columns)\n",
    "\n",
    "\n",
    "print(matches_df.columns)\n",
    "\n",
    "\n",
    "print(medals_matches_players.columns)"
   ]
  },
  {
   "cell_type": "code",
   "execution_count": 6,
   "id": "7186e398-8f41-4333-af05-a6c7914dfd10",
   "metadata": {},
   "outputs": [
    {
     "name": "stderr",
     "output_type": "stream",
     "text": [
      "[Stage 18:===========================================>              (6 + 2) / 8]\r"
     ]
    },
    {
     "name": "stdout",
     "output_type": "stream",
     "text": [
      "+------------------------------------+------------------------------------+------------+------------------------------------+------------------------------------+-------------+-------------------+--------------+---------+------------------------------------+---------------+---------------------+------------+-----------------+--------+-----------------+------------------------+------------+---------------------------------+-----------------+----------------+-----------------------+-----------+--------------------------------+----------------+-------------------+---------------+-------------------+------------------+----------------------+--------------------------+-------------------------+------------------------+-------------------------+---------------------------+-------------------------------+--------------------------------+---------------------------+--------------------------------+-------------------------------+-------------------+--------------------+--------------------------+-------+-------+---------------+----------+-----+\n",
      "|match_id                            |mapid                               |is_team_game|playlist_id                         |game_variant_id                     |is_match_over|completion_date    |match_duration|game_mode|map_variant_id                      |player_gamertag|previous_spartan_rank|spartan_rank|previous_total_xp|total_xp|previous_csr_tier|previous_csr_designation|previous_csr|previous_csr_percent_to_next_tier|previous_csr_rank|current_csr_tier|current_csr_designation|current_csr|current_csr_percent_to_next_tier|current_csr_rank|player_rank_on_team|player_finished|player_average_life|player_total_kills|player_total_headshots|player_total_weapon_damage|player_total_shots_landed|player_total_melee_kills|player_total_melee_damage|player_total_assassinations|player_total_ground_pound_kills|player_total_shoulder_bash_kills|player_total_grenade_damage|player_total_power_weapon_damage|player_total_power_weapon_grabs|player_total_deaths|player_total_assists|player_total_grenade_kills|did_win|team_id|player_gamertag|medal_id  |count|\n",
      "+------------------------------------+------------------------------------+------------+------------------------------------+------------------------------------+-------------+-------------------+--------------+---------+------------------------------------+---------------+---------------------+------------+-----------------+--------+-----------------+------------------------+------------+---------------------------------+-----------------+----------------+-----------------------+-----------+--------------------------------+----------------+-------------------+---------------+-------------------+------------------+----------------------+--------------------------+-------------------------+------------------------+-------------------------+---------------------------+-------------------------------+--------------------------------+---------------------------+--------------------------------+-------------------------------+-------------------+--------------------+--------------------------+-------+-------+---------------+----------+-----+\n",
      "|019411a2-67d4-4e0f-b6b3-554e3b3c5cd1|c7edbf0f-f206-11e4-aa52-24be05e24f7e|NULL        |f72e0ef0-7c4a-4307-af78-8e38dac3fdba|1e473914-46e4-408d-af26-178fb115de76|NULL         |2016-01-20 00:00:00|NULL          |NULL     |9b1c13eb-5ca8-4695-b0da-3fe4859972b1|carlitospatacon|49                   |49          |799666           |801349  |2                |4                       |0           |4                                |NULL             |1               |4                      |0          |78                              |NULL            |7                  |false          |PT25.820186S       |2                 |0                     |195.60958862304688        |22                       |0                       |0.0                      |0                          |0                              |0                               |51.259586334228516         |0.0                             |0                              |7                  |1                   |1                         |0      |1      |EcZachly       |3261908037|10   |\n",
      "|019411a2-67d4-4e0f-b6b3-554e3b3c5cd1|c7edbf0f-f206-11e4-aa52-24be05e24f7e|NULL        |f72e0ef0-7c4a-4307-af78-8e38dac3fdba|1e473914-46e4-408d-af26-178fb115de76|NULL         |2016-01-20 00:00:00|NULL          |NULL     |9b1c13eb-5ca8-4695-b0da-3fe4859972b1|carlitospatacon|49                   |49          |799666           |801349  |2                |4                       |0           |4                                |NULL             |1               |4                      |0          |78                              |NULL            |7                  |false          |PT25.820186S       |2                 |0                     |195.60958862304688        |22                       |0                       |0.0                      |0                          |0                              |0                               |51.259586334228516         |0.0                             |0                              |7                  |1                   |1                         |0      |1      |EcZachly       |2078758684|2    |\n",
      "|019411a2-67d4-4e0f-b6b3-554e3b3c5cd1|c7edbf0f-f206-11e4-aa52-24be05e24f7e|NULL        |f72e0ef0-7c4a-4307-af78-8e38dac3fdba|1e473914-46e4-408d-af26-178fb115de76|NULL         |2016-01-20 00:00:00|NULL          |NULL     |9b1c13eb-5ca8-4695-b0da-3fe4859972b1|carlitospatacon|49                   |49          |799666           |801349  |2                |4                       |0           |4                                |NULL             |1               |4                      |0          |78                              |NULL            |7                  |false          |PT25.820186S       |2                 |0                     |195.60958862304688        |22                       |0                       |0.0                      |0                          |0                              |0                               |51.259586334228516         |0.0                             |0                              |7                  |1                   |1                         |0      |1      |EcZachly       |2763748638|1    |\n",
      "|019411a2-67d4-4e0f-b6b3-554e3b3c5cd1|c7edbf0f-f206-11e4-aa52-24be05e24f7e|NULL        |f72e0ef0-7c4a-4307-af78-8e38dac3fdba|1e473914-46e4-408d-af26-178fb115de76|NULL         |2016-01-20 00:00:00|NULL          |NULL     |9b1c13eb-5ca8-4695-b0da-3fe4859972b1|carlitospatacon|49                   |49          |799666           |801349  |2                |4                       |0           |4                                |NULL             |1               |4                      |0          |78                              |NULL            |7                  |false          |PT25.820186S       |2                 |0                     |195.60958862304688        |22                       |0                       |0.0                      |0                          |0                              |0                               |51.259586334228516         |0.0                             |0                              |7                  |1                   |1                         |0      |1      |EcZachly       |2782465081|1    |\n",
      "|019411a2-67d4-4e0f-b6b3-554e3b3c5cd1|c7edbf0f-f206-11e4-aa52-24be05e24f7e|NULL        |f72e0ef0-7c4a-4307-af78-8e38dac3fdba|1e473914-46e4-408d-af26-178fb115de76|NULL         |2016-01-20 00:00:00|NULL          |NULL     |9b1c13eb-5ca8-4695-b0da-3fe4859972b1|carlitospatacon|49                   |49          |799666           |801349  |2                |4                       |0           |4                                |NULL             |1               |4                      |0          |78                              |NULL            |7                  |false          |PT25.820186S       |2                 |0                     |195.60958862304688        |22                       |0                       |0.0                      |0                          |0                              |0                               |51.259586334228516         |0.0                             |0                              |7                  |1                   |1                         |0      |1      |EcZachly       |250435527 |1    |\n",
      "|019411a2-67d4-4e0f-b6b3-554e3b3c5cd1|c7edbf0f-f206-11e4-aa52-24be05e24f7e|NULL        |f72e0ef0-7c4a-4307-af78-8e38dac3fdba|1e473914-46e4-408d-af26-178fb115de76|NULL         |2016-01-20 00:00:00|NULL          |NULL     |9b1c13eb-5ca8-4695-b0da-3fe4859972b1|carlitospatacon|49                   |49          |799666           |801349  |2                |4                       |0           |4                                |NULL             |1               |4                      |0          |78                              |NULL            |7                  |false          |PT25.820186S       |2                 |0                     |195.60958862304688        |22                       |0                       |0.0                      |0                          |0                              |0                               |51.259586334228516         |0.0                             |0                              |7                  |1                   |1                         |0      |1      |EcZachly       |2494364276|1    |\n",
      "|019411a2-67d4-4e0f-b6b3-554e3b3c5cd1|c7edbf0f-f206-11e4-aa52-24be05e24f7e|NULL        |f72e0ef0-7c4a-4307-af78-8e38dac3fdba|1e473914-46e4-408d-af26-178fb115de76|NULL         |2016-01-20 00:00:00|NULL          |NULL     |9b1c13eb-5ca8-4695-b0da-3fe4859972b1|carlitospatacon|49                   |49          |799666           |801349  |2                |4                       |0           |4                                |NULL             |1               |4                      |0          |78                              |NULL            |7                  |false          |PT25.820186S       |2                 |0                     |195.60958862304688        |22                       |0                       |0.0                      |0                          |0                              |0                               |51.259586334228516         |0.0                             |0                              |7                  |1                   |1                         |0      |1      |EcZachly       |2966496172|1    |\n",
      "|019411a2-67d4-4e0f-b6b3-554e3b3c5cd1|c7edbf0f-f206-11e4-aa52-24be05e24f7e|NULL        |f72e0ef0-7c4a-4307-af78-8e38dac3fdba|1e473914-46e4-408d-af26-178fb115de76|NULL         |2016-01-20 00:00:00|NULL          |NULL     |9b1c13eb-5ca8-4695-b0da-3fe4859972b1|carlitospatacon|49                   |49          |799666           |801349  |2                |4                       |0           |4                                |NULL             |1               |4                      |0          |78                              |NULL            |7                  |false          |PT25.820186S       |2                 |0                     |195.60958862304688        |22                       |0                       |0.0                      |0                          |0                              |0                               |51.259586334228516         |0.0                             |0                              |7                  |1                   |1                         |0      |1      |ILLICIT 117    |298813630 |1    |\n",
      "|019411a2-67d4-4e0f-b6b3-554e3b3c5cd1|c7edbf0f-f206-11e4-aa52-24be05e24f7e|NULL        |f72e0ef0-7c4a-4307-af78-8e38dac3fdba|1e473914-46e4-408d-af26-178fb115de76|NULL         |2016-01-20 00:00:00|NULL          |NULL     |9b1c13eb-5ca8-4695-b0da-3fe4859972b1|carlitospatacon|49                   |49          |799666           |801349  |2                |4                       |0           |4                                |NULL             |1               |4                      |0          |78                              |NULL            |7                  |false          |PT25.820186S       |2                 |0                     |195.60958862304688        |22                       |0                       |0.0                      |0                          |0                              |0                               |51.259586334228516         |0.0                             |0                              |7                  |1                   |1                         |0      |1      |ILLICIT 117    |3001183151|1    |\n",
      "|019411a2-67d4-4e0f-b6b3-554e3b3c5cd1|c7edbf0f-f206-11e4-aa52-24be05e24f7e|NULL        |f72e0ef0-7c4a-4307-af78-8e38dac3fdba|1e473914-46e4-408d-af26-178fb115de76|NULL         |2016-01-20 00:00:00|NULL          |NULL     |9b1c13eb-5ca8-4695-b0da-3fe4859972b1|carlitospatacon|49                   |49          |799666           |801349  |2                |4                       |0           |4                                |NULL             |1               |4                      |0          |78                              |NULL            |7                  |false          |PT25.820186S       |2                 |0                     |195.60958862304688        |22                       |0                       |0.0                      |0                          |0                              |0                               |51.259586334228516         |0.0                             |0                              |7                  |1                   |1                         |0      |1      |ILLICIT 117    |1618319591|1    |\n",
      "+------------------------------------+------------------------------------+------------+------------------------------------+------------------------------------+-------------+-------------------+--------------+---------+------------------------------------+---------------+---------------------+------------+-----------------+--------+-----------------+------------------------+------------+---------------------------------+-----------------+----------------+-----------------------+-----------+--------------------------------+----------------+-------------------+---------------+-------------------+------------------+----------------------+--------------------------+-------------------------+------------------------+-------------------------+---------------------------+-------------------------------+--------------------------------+---------------------------+--------------------------------+-------------------------------+-------------------+--------------------+--------------------------+-------+-------+---------------+----------+-----+\n",
      "only showing top 10 rows\n",
      "\n",
      "['match_id', 'mapid', 'is_team_game', 'playlist_id', 'game_variant_id', 'is_match_over', 'completion_date', 'match_duration', 'game_mode', 'map_variant_id', 'player_gamertag', 'previous_spartan_rank', 'spartan_rank', 'previous_total_xp', 'total_xp', 'previous_csr_tier', 'previous_csr_designation', 'previous_csr', 'previous_csr_percent_to_next_tier', 'previous_csr_rank', 'current_csr_tier', 'current_csr_designation', 'current_csr', 'current_csr_percent_to_next_tier', 'current_csr_rank', 'player_rank_on_team', 'player_finished', 'player_average_life', 'player_total_kills', 'player_total_headshots', 'player_total_weapon_damage', 'player_total_shots_landed', 'player_total_melee_kills', 'player_total_melee_damage', 'player_total_assassinations', 'player_total_ground_pound_kills', 'player_total_shoulder_bash_kills', 'player_total_grenade_damage', 'player_total_power_weapon_damage', 'player_total_power_weapon_grabs', 'player_total_deaths', 'player_total_assists', 'player_total_grenade_kills', 'did_win', 'team_id', 'player_gamertag', 'medal_id', 'count']\n"
     ]
    },
    {
     "name": "stderr",
     "output_type": "stream",
     "text": [
      "                                                                                \r"
     ]
    }
   ],
   "source": [
    "# Bucket join match_details, matches, and medal_matches_players on match_id with 16 buckets\n",
    "spark.conf.set(\"spark.sql.autoBroadcastJoinThreshold\", -1)\n",
    "\n",
    "match_details_df = match_details_df.repartition(16, \"match_id\")\n",
    "matches_df = matches_df.repartition(16, \"match_id\")\n",
    "medals_matches_players = medals_matches_players.repartition(16, \"match_id\")\n",
    "\n",
    "result = match_details_df.join(matches_df, \"match_id\").join(medals_matches_players, \"match_id\")\n",
    "result.show(10,  truncate=False)\n",
    "print(result.columns)"
   ]
  },
  {
   "cell_type": "code",
   "execution_count": 7,
   "id": "7f8e5502-d8f9-463e-965a-af1461dc5774",
   "metadata": {},
   "outputs": [
    {
     "name": "stderr",
     "output_type": "stream",
     "text": [
      "                                                                                \r"
     ]
    },
    {
     "name": "stdout",
     "output_type": "stream",
     "text": [
      "+---------------+------------------+\n",
      "|player_gamertag|most_kills_in_game|\n",
      "+---------------+------------------+\n",
      "|zombiesrhunters|              56.0|\n",
      "+---------------+------------------+\n",
      "only showing top 1 row\n",
      "\n"
     ]
    },
    {
     "name": "stderr",
     "output_type": "stream",
     "text": [
      "                                                                                \r"
     ]
    },
    {
     "name": "stdout",
     "output_type": "stream",
     "text": [
      "+--------------------+--------------------+\n",
      "|         playlist_id|most_played_playlist|\n",
      "+--------------------+--------------------+\n",
      "|f72e0ef0-7c4a-430...|                7640|\n",
      "+--------------------+--------------------+\n",
      "only showing top 1 row\n",
      "\n"
     ]
    },
    {
     "name": "stderr",
     "output_type": "stream",
     "text": [
      "[Stage 53:=========================================>            (152 + 8) / 200]\r"
     ]
    },
    {
     "name": "stdout",
     "output_type": "stream",
     "text": [
      "+--------------------+---------------+\n",
      "|               mapid|most_played_map|\n",
      "+--------------------+---------------+\n",
      "|c7edbf0f-f206-11e...|           7032|\n",
      "+--------------------+---------------+\n",
      "only showing top 1 row\n",
      "\n"
     ]
    },
    {
     "name": "stderr",
     "output_type": "stream",
     "text": [
      "                                                                                \r"
     ]
    }
   ],
   "source": [
    "# Aggregate the joined data frame to figure out questions like:\n",
    "from pyspark.sql import SparkSession\n",
    "from pyspark.sql.functions import avg, desc, max\n",
    "from pyspark.sql import functions as F\n",
    "\n",
    "\n",
    "# Drop duplicate player_gamertag columns before aggregation\n",
    "result = result.drop(matches_df[\"player_gamertag\"]) \n",
    "result = result.drop(matches_df[\"match_id\"])  # Keep only one\n",
    "result = result.drop(medals_matches_players[\"match_id\"])  # Keep only one\n",
    "\n",
    "\n",
    "# Which player averages the most kills per game?\n",
    "\n",
    "most_kills_df = (\n",
    "    result.groupBy(\"player_gamertag\")\n",
    "    .agg(F.avg(\"player_total_kills\").alias(\"most_kills_in_game\"))\n",
    "    .orderBy(F.desc(\"most_kills_in_game\"))  # Order by the most kills in descending order\n",
    ")\n",
    "\n",
    "most_kills_df.show(1)\n",
    "\n",
    "\n",
    "# Which playlist gets played the most?\n",
    "\n",
    "max_playlist_played = (\n",
    "    result.groupBy(\"playlist_id\")\n",
    "    .agg(F.countDistinct(\"match_id\").alias(\"most_played_playlist\"))\n",
    "    .orderBy(desc(\"most_played_playlist\"))\n",
    ")\n",
    "\n",
    "max_playlist_played.show(1)\n",
    "    \n",
    "# Which map gets played the most?\n",
    "\n",
    "max_map_played = (\n",
    "    result.groupBy(\"mapid\")\n",
    "    .agg(F.countDistinct(\"match_id\").alias(\"most_played_map\"))\n",
    "    .orderBy(desc(\"most_played_map\"))\n",
    ")\n",
    "\n",
    "max_map_played.show(1)"
   ]
  },
  {
   "cell_type": "code",
   "execution_count": 8,
   "id": "dd95729b-fb44-4d3b-8737-541b3a2eba5a",
   "metadata": {},
   "outputs": [
    {
     "name": "stderr",
     "output_type": "stream",
     "text": [
      "25/01/31 02:55:18 WARN SparkSession: Using an existing Spark session; only runtime SQL configurations will take effect.\n"
     ]
    },
    {
     "name": "stdout",
     "output_type": "stream",
     "text": [
      "Sorting by playlist_id:\n",
      "== Parsed Logical Plan ==\n",
      "'Sort ['playlist_id ASC NULLS FIRST], false\n",
      "+- Project [match_id#129, mapid#130, is_team_game#131, playlist_id#132, game_variant_id#133, is_match_over#134, completion_date#135, match_duration#136, game_mode#137, map_variant_id#138, previous_spartan_rank#42, spartan_rank#43, previous_total_xp#44, total_xp#45, previous_csr_tier#46, previous_csr_designation#47, previous_csr#48, previous_csr_percent_to_next_tier#49, previous_csr_rank#50, current_csr_tier#51, current_csr_designation#52, current_csr#53, current_csr_percent_to_next_tier#54, current_csr_rank#55, ... 23 more fields]\n",
      "   +- Project [match_id#129, mapid#130, is_team_game#131, playlist_id#132, game_variant_id#133, is_match_over#134, completion_date#135, match_duration#136, game_mode#137, map_variant_id#138, player_gamertag#41, previous_spartan_rank#42, spartan_rank#43, previous_total_xp#44, total_xp#45, previous_csr_tier#46, previous_csr_designation#47, previous_csr#48, previous_csr_percent_to_next_tier#49, previous_csr_rank#50, current_csr_tier#51, current_csr_designation#52, current_csr#53, current_csr_percent_to_next_tier#54, ... 24 more fields]\n",
      "      +- Join Inner, (match_id#129 = match_id#166)\n",
      "         :- Project [match_id#129, mapid#130, is_team_game#131, playlist_id#132, game_variant_id#133, is_match_over#134, completion_date#135, match_duration#136, game_mode#137, map_variant_id#138, player_gamertag#41, previous_spartan_rank#42, spartan_rank#43, previous_total_xp#44, total_xp#45, previous_csr_tier#46, previous_csr_designation#47, previous_csr#48, previous_csr_percent_to_next_tier#49, previous_csr_rank#50, current_csr_tier#51, current_csr_designation#52, current_csr#53, current_csr_percent_to_next_tier#54, ... 21 more fields]\n",
      "         :  +- Join Inner, (match_id#129 = match_id#40)\n",
      "         :     :- RepartitionByExpression [match_id#129], 16\n",
      "         :     :  +- Relation [match_id#129,mapid#130,is_team_game#131,playlist_id#132,game_variant_id#133,is_match_over#134,completion_date#135,match_duration#136,game_mode#137,map_variant_id#138] csv\n",
      "         :     +- RepartitionByExpression [match_id#40], 16\n",
      "         :        +- Relation [match_id#40,player_gamertag#41,previous_spartan_rank#42,spartan_rank#43,previous_total_xp#44,total_xp#45,previous_csr_tier#46,previous_csr_designation#47,previous_csr#48,previous_csr_percent_to_next_tier#49,previous_csr_rank#50,current_csr_tier#51,current_csr_designation#52,current_csr#53,current_csr_percent_to_next_tier#54,current_csr_rank#55,player_rank_on_team#56,player_finished#57,player_average_life#58,player_total_kills#59,player_total_headshots#60,player_total_weapon_damage#61,player_total_shots_landed#62,player_total_melee_kills#63,... 12 more fields] csv\n",
      "         +- RepartitionByExpression [match_id#166], 16\n",
      "            +- Relation [match_id#166,player_gamertag#167,medal_id#168L,count#169] csv\n",
      "\n",
      "== Analyzed Logical Plan ==\n",
      "match_id: string, mapid: string, is_team_game: boolean, playlist_id: string, game_variant_id: string, is_match_over: boolean, completion_date: timestamp, match_duration: string, game_mode: string, map_variant_id: string, previous_spartan_rank: int, spartan_rank: int, previous_total_xp: int, total_xp: int, previous_csr_tier: int, previous_csr_designation: int, previous_csr: int, previous_csr_percent_to_next_tier: int, previous_csr_rank: int, current_csr_tier: int, current_csr_designation: int, current_csr: int, current_csr_percent_to_next_tier: int, current_csr_rank: int, ... 23 more fields\n",
      "Sort [playlist_id#132 ASC NULLS FIRST], false\n",
      "+- Project [match_id#129, mapid#130, is_team_game#131, playlist_id#132, game_variant_id#133, is_match_over#134, completion_date#135, match_duration#136, game_mode#137, map_variant_id#138, previous_spartan_rank#42, spartan_rank#43, previous_total_xp#44, total_xp#45, previous_csr_tier#46, previous_csr_designation#47, previous_csr#48, previous_csr_percent_to_next_tier#49, previous_csr_rank#50, current_csr_tier#51, current_csr_designation#52, current_csr#53, current_csr_percent_to_next_tier#54, current_csr_rank#55, ... 23 more fields]\n",
      "   +- Project [match_id#129, mapid#130, is_team_game#131, playlist_id#132, game_variant_id#133, is_match_over#134, completion_date#135, match_duration#136, game_mode#137, map_variant_id#138, player_gamertag#41, previous_spartan_rank#42, spartan_rank#43, previous_total_xp#44, total_xp#45, previous_csr_tier#46, previous_csr_designation#47, previous_csr#48, previous_csr_percent_to_next_tier#49, previous_csr_rank#50, current_csr_tier#51, current_csr_designation#52, current_csr#53, current_csr_percent_to_next_tier#54, ... 24 more fields]\n",
      "      +- Join Inner, (match_id#129 = match_id#166)\n",
      "         :- Project [match_id#129, mapid#130, is_team_game#131, playlist_id#132, game_variant_id#133, is_match_over#134, completion_date#135, match_duration#136, game_mode#137, map_variant_id#138, player_gamertag#41, previous_spartan_rank#42, spartan_rank#43, previous_total_xp#44, total_xp#45, previous_csr_tier#46, previous_csr_designation#47, previous_csr#48, previous_csr_percent_to_next_tier#49, previous_csr_rank#50, current_csr_tier#51, current_csr_designation#52, current_csr#53, current_csr_percent_to_next_tier#54, ... 21 more fields]\n",
      "         :  +- Join Inner, (match_id#129 = match_id#40)\n",
      "         :     :- RepartitionByExpression [match_id#129], 16\n",
      "         :     :  +- Relation [match_id#129,mapid#130,is_team_game#131,playlist_id#132,game_variant_id#133,is_match_over#134,completion_date#135,match_duration#136,game_mode#137,map_variant_id#138] csv\n",
      "         :     +- RepartitionByExpression [match_id#40], 16\n",
      "         :        +- Relation [match_id#40,player_gamertag#41,previous_spartan_rank#42,spartan_rank#43,previous_total_xp#44,total_xp#45,previous_csr_tier#46,previous_csr_designation#47,previous_csr#48,previous_csr_percent_to_next_tier#49,previous_csr_rank#50,current_csr_tier#51,current_csr_designation#52,current_csr#53,current_csr_percent_to_next_tier#54,current_csr_rank#55,player_rank_on_team#56,player_finished#57,player_average_life#58,player_total_kills#59,player_total_headshots#60,player_total_weapon_damage#61,player_total_shots_landed#62,player_total_melee_kills#63,... 12 more fields] csv\n",
      "         +- RepartitionByExpression [match_id#166], 16\n",
      "            +- Relation [match_id#166,player_gamertag#167,medal_id#168L,count#169] csv\n",
      "\n",
      "== Optimized Logical Plan ==\n",
      "Sort [playlist_id#132 ASC NULLS FIRST], false\n",
      "+- Project [match_id#129, mapid#130, is_team_game#131, playlist_id#132, game_variant_id#133, is_match_over#134, completion_date#135, match_duration#136, game_mode#137, map_variant_id#138, previous_spartan_rank#42, spartan_rank#43, previous_total_xp#44, total_xp#45, previous_csr_tier#46, previous_csr_designation#47, previous_csr#48, previous_csr_percent_to_next_tier#49, previous_csr_rank#50, current_csr_tier#51, current_csr_designation#52, current_csr#53, current_csr_percent_to_next_tier#54, current_csr_rank#55, ... 23 more fields]\n",
      "   +- Join Inner, (match_id#129 = match_id#166)\n",
      "      :- Project [match_id#129, mapid#130, is_team_game#131, playlist_id#132, game_variant_id#133, is_match_over#134, completion_date#135, match_duration#136, game_mode#137, map_variant_id#138, previous_spartan_rank#42, spartan_rank#43, previous_total_xp#44, total_xp#45, previous_csr_tier#46, previous_csr_designation#47, previous_csr#48, previous_csr_percent_to_next_tier#49, previous_csr_rank#50, current_csr_tier#51, current_csr_designation#52, current_csr#53, current_csr_percent_to_next_tier#54, current_csr_rank#55, ... 20 more fields]\n",
      "      :  +- Join Inner, (match_id#129 = match_id#40)\n",
      "      :     :- RepartitionByExpression [match_id#129], 16\n",
      "      :     :  +- Filter isnotnull(match_id#129)\n",
      "      :     :     +- Relation [match_id#129,mapid#130,is_team_game#131,playlist_id#132,game_variant_id#133,is_match_over#134,completion_date#135,match_duration#136,game_mode#137,map_variant_id#138] csv\n",
      "      :     +- RepartitionByExpression [match_id#40], 16\n",
      "      :        +- Project [match_id#40, previous_spartan_rank#42, spartan_rank#43, previous_total_xp#44, total_xp#45, previous_csr_tier#46, previous_csr_designation#47, previous_csr#48, previous_csr_percent_to_next_tier#49, previous_csr_rank#50, current_csr_tier#51, current_csr_designation#52, current_csr#53, current_csr_percent_to_next_tier#54, current_csr_rank#55, player_rank_on_team#56, player_finished#57, player_average_life#58, player_total_kills#59, player_total_headshots#60, player_total_weapon_damage#61, player_total_shots_landed#62, player_total_melee_kills#63, player_total_melee_damage#64, ... 11 more fields]\n",
      "      :           +- Filter isnotnull(match_id#40)\n",
      "      :              +- Relation [match_id#40,player_gamertag#41,previous_spartan_rank#42,spartan_rank#43,previous_total_xp#44,total_xp#45,previous_csr_tier#46,previous_csr_designation#47,previous_csr#48,previous_csr_percent_to_next_tier#49,previous_csr_rank#50,current_csr_tier#51,current_csr_designation#52,current_csr#53,current_csr_percent_to_next_tier#54,current_csr_rank#55,player_rank_on_team#56,player_finished#57,player_average_life#58,player_total_kills#59,player_total_headshots#60,player_total_weapon_damage#61,player_total_shots_landed#62,player_total_melee_kills#63,... 12 more fields] csv\n",
      "      +- RepartitionByExpression [match_id#166], 16\n",
      "         +- Filter isnotnull(match_id#166)\n",
      "            +- Relation [match_id#166,player_gamertag#167,medal_id#168L,count#169] csv\n",
      "\n",
      "== Physical Plan ==\n",
      "AdaptiveSparkPlan isFinalPlan=false\n",
      "+- Sort [playlist_id#132 ASC NULLS FIRST], false, 0\n",
      "   +- Project [match_id#129, mapid#130, is_team_game#131, playlist_id#132, game_variant_id#133, is_match_over#134, completion_date#135, match_duration#136, game_mode#137, map_variant_id#138, previous_spartan_rank#42, spartan_rank#43, previous_total_xp#44, total_xp#45, previous_csr_tier#46, previous_csr_designation#47, previous_csr#48, previous_csr_percent_to_next_tier#49, previous_csr_rank#50, current_csr_tier#51, current_csr_designation#52, current_csr#53, current_csr_percent_to_next_tier#54, current_csr_rank#55, ... 23 more fields]\n",
      "      +- SortMergeJoin [match_id#129], [match_id#166], Inner\n",
      "         :- Project [match_id#129, mapid#130, is_team_game#131, playlist_id#132, game_variant_id#133, is_match_over#134, completion_date#135, match_duration#136, game_mode#137, map_variant_id#138, previous_spartan_rank#42, spartan_rank#43, previous_total_xp#44, total_xp#45, previous_csr_tier#46, previous_csr_designation#47, previous_csr#48, previous_csr_percent_to_next_tier#49, previous_csr_rank#50, current_csr_tier#51, current_csr_designation#52, current_csr#53, current_csr_percent_to_next_tier#54, current_csr_rank#55, ... 20 more fields]\n",
      "         :  +- SortMergeJoin [match_id#129], [match_id#40], Inner\n",
      "         :     :- Sort [match_id#129 ASC NULLS FIRST], false, 0\n",
      "         :     :  +- Exchange hashpartitioning(match_id#129, 200), REPARTITION_BY_NUM, [plan_id=1832]\n",
      "         :     :     +- Filter isnotnull(match_id#129)\n",
      "         :     :        +- FileScan csv [match_id#129,mapid#130,is_team_game#131,playlist_id#132,game_variant_id#133,is_match_over#134,completion_date#135,match_duration#136,game_mode#137,map_variant_id#138] Batched: false, DataFilters: [isnotnull(match_id#129)], Format: CSV, Location: InMemoryFileIndex(1 paths)[file:/home/iceberg/notebooks/matches.csv], PartitionFilters: [], PushedFilters: [IsNotNull(match_id)], ReadSchema: struct<match_id:string,mapid:string,is_team_game:boolean,playlist_id:string,game_variant_id:strin...\n",
      "         :     +- Sort [match_id#40 ASC NULLS FIRST], false, 0\n",
      "         :        +- Exchange hashpartitioning(match_id#40, 200), REPARTITION_BY_NUM, [plan_id=1833]\n",
      "         :           +- Filter isnotnull(match_id#40)\n",
      "         :              +- FileScan csv [match_id#40,previous_spartan_rank#42,spartan_rank#43,previous_total_xp#44,total_xp#45,previous_csr_tier#46,previous_csr_designation#47,previous_csr#48,previous_csr_percent_to_next_tier#49,previous_csr_rank#50,current_csr_tier#51,current_csr_designation#52,current_csr#53,current_csr_percent_to_next_tier#54,current_csr_rank#55,player_rank_on_team#56,player_finished#57,player_average_life#58,player_total_kills#59,player_total_headshots#60,player_total_weapon_damage#61,player_total_shots_landed#62,player_total_melee_kills#63,player_total_melee_damage#64,... 11 more fields] Batched: false, DataFilters: [isnotnull(match_id#40)], Format: CSV, Location: InMemoryFileIndex(1 paths)[file:/home/iceberg/notebooks/match_details.csv], PartitionFilters: [], PushedFilters: [IsNotNull(match_id)], ReadSchema: struct<match_id:string,previous_spartan_rank:int,spartan_rank:int,previous_total_xp:int,total_xp:...\n",
      "         +- Sort [match_id#166 ASC NULLS FIRST], false, 0\n",
      "            +- Exchange hashpartitioning(match_id#166, 200), REPARTITION_BY_NUM, [plan_id=1840]\n",
      "               +- Filter isnotnull(match_id#166)\n",
      "                  +- FileScan csv [match_id#166,player_gamertag#167,medal_id#168L,count#169] Batched: false, DataFilters: [isnotnull(match_id#166)], Format: CSV, Location: InMemoryFileIndex(1 paths)[file:/home/iceberg/notebooks/medals_matches_players.csv], PartitionFilters: [], PushedFilters: [IsNotNull(match_id)], ReadSchema: struct<match_id:string,player_gamertag:string,medal_id:bigint,count:int>\n",
      "\n",
      "\n",
      "Sorting by map:\n",
      "== Parsed Logical Plan ==\n",
      "'Sort ['mapid ASC NULLS FIRST], false\n",
      "+- Project [match_id#129, mapid#130, is_team_game#131, playlist_id#132, game_variant_id#133, is_match_over#134, completion_date#135, match_duration#136, game_mode#137, map_variant_id#138, previous_spartan_rank#42, spartan_rank#43, previous_total_xp#44, total_xp#45, previous_csr_tier#46, previous_csr_designation#47, previous_csr#48, previous_csr_percent_to_next_tier#49, previous_csr_rank#50, current_csr_tier#51, current_csr_designation#52, current_csr#53, current_csr_percent_to_next_tier#54, current_csr_rank#55, ... 23 more fields]\n",
      "   +- Project [match_id#129, mapid#130, is_team_game#131, playlist_id#132, game_variant_id#133, is_match_over#134, completion_date#135, match_duration#136, game_mode#137, map_variant_id#138, player_gamertag#41, previous_spartan_rank#42, spartan_rank#43, previous_total_xp#44, total_xp#45, previous_csr_tier#46, previous_csr_designation#47, previous_csr#48, previous_csr_percent_to_next_tier#49, previous_csr_rank#50, current_csr_tier#51, current_csr_designation#52, current_csr#53, current_csr_percent_to_next_tier#54, ... 24 more fields]\n",
      "      +- Join Inner, (match_id#129 = match_id#166)\n",
      "         :- Project [match_id#129, mapid#130, is_team_game#131, playlist_id#132, game_variant_id#133, is_match_over#134, completion_date#135, match_duration#136, game_mode#137, map_variant_id#138, player_gamertag#41, previous_spartan_rank#42, spartan_rank#43, previous_total_xp#44, total_xp#45, previous_csr_tier#46, previous_csr_designation#47, previous_csr#48, previous_csr_percent_to_next_tier#49, previous_csr_rank#50, current_csr_tier#51, current_csr_designation#52, current_csr#53, current_csr_percent_to_next_tier#54, ... 21 more fields]\n",
      "         :  +- Join Inner, (match_id#129 = match_id#40)\n",
      "         :     :- RepartitionByExpression [match_id#129], 16\n",
      "         :     :  +- Relation [match_id#129,mapid#130,is_team_game#131,playlist_id#132,game_variant_id#133,is_match_over#134,completion_date#135,match_duration#136,game_mode#137,map_variant_id#138] csv\n",
      "         :     +- RepartitionByExpression [match_id#40], 16\n",
      "         :        +- Relation [match_id#40,player_gamertag#41,previous_spartan_rank#42,spartan_rank#43,previous_total_xp#44,total_xp#45,previous_csr_tier#46,previous_csr_designation#47,previous_csr#48,previous_csr_percent_to_next_tier#49,previous_csr_rank#50,current_csr_tier#51,current_csr_designation#52,current_csr#53,current_csr_percent_to_next_tier#54,current_csr_rank#55,player_rank_on_team#56,player_finished#57,player_average_life#58,player_total_kills#59,player_total_headshots#60,player_total_weapon_damage#61,player_total_shots_landed#62,player_total_melee_kills#63,... 12 more fields] csv\n",
      "         +- RepartitionByExpression [match_id#166], 16\n",
      "            +- Relation [match_id#166,player_gamertag#167,medal_id#168L,count#169] csv\n",
      "\n",
      "== Analyzed Logical Plan ==\n",
      "match_id: string, mapid: string, is_team_game: boolean, playlist_id: string, game_variant_id: string, is_match_over: boolean, completion_date: timestamp, match_duration: string, game_mode: string, map_variant_id: string, previous_spartan_rank: int, spartan_rank: int, previous_total_xp: int, total_xp: int, previous_csr_tier: int, previous_csr_designation: int, previous_csr: int, previous_csr_percent_to_next_tier: int, previous_csr_rank: int, current_csr_tier: int, current_csr_designation: int, current_csr: int, current_csr_percent_to_next_tier: int, current_csr_rank: int, ... 23 more fields\n",
      "Sort [mapid#130 ASC NULLS FIRST], false\n",
      "+- Project [match_id#129, mapid#130, is_team_game#131, playlist_id#132, game_variant_id#133, is_match_over#134, completion_date#135, match_duration#136, game_mode#137, map_variant_id#138, previous_spartan_rank#42, spartan_rank#43, previous_total_xp#44, total_xp#45, previous_csr_tier#46, previous_csr_designation#47, previous_csr#48, previous_csr_percent_to_next_tier#49, previous_csr_rank#50, current_csr_tier#51, current_csr_designation#52, current_csr#53, current_csr_percent_to_next_tier#54, current_csr_rank#55, ... 23 more fields]\n",
      "   +- Project [match_id#129, mapid#130, is_team_game#131, playlist_id#132, game_variant_id#133, is_match_over#134, completion_date#135, match_duration#136, game_mode#137, map_variant_id#138, player_gamertag#41, previous_spartan_rank#42, spartan_rank#43, previous_total_xp#44, total_xp#45, previous_csr_tier#46, previous_csr_designation#47, previous_csr#48, previous_csr_percent_to_next_tier#49, previous_csr_rank#50, current_csr_tier#51, current_csr_designation#52, current_csr#53, current_csr_percent_to_next_tier#54, ... 24 more fields]\n",
      "      +- Join Inner, (match_id#129 = match_id#166)\n",
      "         :- Project [match_id#129, mapid#130, is_team_game#131, playlist_id#132, game_variant_id#133, is_match_over#134, completion_date#135, match_duration#136, game_mode#137, map_variant_id#138, player_gamertag#41, previous_spartan_rank#42, spartan_rank#43, previous_total_xp#44, total_xp#45, previous_csr_tier#46, previous_csr_designation#47, previous_csr#48, previous_csr_percent_to_next_tier#49, previous_csr_rank#50, current_csr_tier#51, current_csr_designation#52, current_csr#53, current_csr_percent_to_next_tier#54, ... 21 more fields]\n",
      "         :  +- Join Inner, (match_id#129 = match_id#40)\n",
      "         :     :- RepartitionByExpression [match_id#129], 16\n",
      "         :     :  +- Relation [match_id#129,mapid#130,is_team_game#131,playlist_id#132,game_variant_id#133,is_match_over#134,completion_date#135,match_duration#136,game_mode#137,map_variant_id#138] csv\n",
      "         :     +- RepartitionByExpression [match_id#40], 16\n",
      "         :        +- Relation [match_id#40,player_gamertag#41,previous_spartan_rank#42,spartan_rank#43,previous_total_xp#44,total_xp#45,previous_csr_tier#46,previous_csr_designation#47,previous_csr#48,previous_csr_percent_to_next_tier#49,previous_csr_rank#50,current_csr_tier#51,current_csr_designation#52,current_csr#53,current_csr_percent_to_next_tier#54,current_csr_rank#55,player_rank_on_team#56,player_finished#57,player_average_life#58,player_total_kills#59,player_total_headshots#60,player_total_weapon_damage#61,player_total_shots_landed#62,player_total_melee_kills#63,... 12 more fields] csv\n",
      "         +- RepartitionByExpression [match_id#166], 16\n",
      "            +- Relation [match_id#166,player_gamertag#167,medal_id#168L,count#169] csv\n",
      "\n",
      "== Optimized Logical Plan ==\n",
      "Sort [mapid#130 ASC NULLS FIRST], false\n",
      "+- Project [match_id#129, mapid#130, is_team_game#131, playlist_id#132, game_variant_id#133, is_match_over#134, completion_date#135, match_duration#136, game_mode#137, map_variant_id#138, previous_spartan_rank#42, spartan_rank#43, previous_total_xp#44, total_xp#45, previous_csr_tier#46, previous_csr_designation#47, previous_csr#48, previous_csr_percent_to_next_tier#49, previous_csr_rank#50, current_csr_tier#51, current_csr_designation#52, current_csr#53, current_csr_percent_to_next_tier#54, current_csr_rank#55, ... 23 more fields]\n",
      "   +- Join Inner, (match_id#129 = match_id#166)\n",
      "      :- Project [match_id#129, mapid#130, is_team_game#131, playlist_id#132, game_variant_id#133, is_match_over#134, completion_date#135, match_duration#136, game_mode#137, map_variant_id#138, previous_spartan_rank#42, spartan_rank#43, previous_total_xp#44, total_xp#45, previous_csr_tier#46, previous_csr_designation#47, previous_csr#48, previous_csr_percent_to_next_tier#49, previous_csr_rank#50, current_csr_tier#51, current_csr_designation#52, current_csr#53, current_csr_percent_to_next_tier#54, current_csr_rank#55, ... 20 more fields]\n",
      "      :  +- Join Inner, (match_id#129 = match_id#40)\n",
      "      :     :- RepartitionByExpression [match_id#129], 16\n",
      "      :     :  +- Filter isnotnull(match_id#129)\n",
      "      :     :     +- Relation [match_id#129,mapid#130,is_team_game#131,playlist_id#132,game_variant_id#133,is_match_over#134,completion_date#135,match_duration#136,game_mode#137,map_variant_id#138] csv\n",
      "      :     +- RepartitionByExpression [match_id#40], 16\n",
      "      :        +- Project [match_id#40, previous_spartan_rank#42, spartan_rank#43, previous_total_xp#44, total_xp#45, previous_csr_tier#46, previous_csr_designation#47, previous_csr#48, previous_csr_percent_to_next_tier#49, previous_csr_rank#50, current_csr_tier#51, current_csr_designation#52, current_csr#53, current_csr_percent_to_next_tier#54, current_csr_rank#55, player_rank_on_team#56, player_finished#57, player_average_life#58, player_total_kills#59, player_total_headshots#60, player_total_weapon_damage#61, player_total_shots_landed#62, player_total_melee_kills#63, player_total_melee_damage#64, ... 11 more fields]\n",
      "      :           +- Filter isnotnull(match_id#40)\n",
      "      :              +- Relation [match_id#40,player_gamertag#41,previous_spartan_rank#42,spartan_rank#43,previous_total_xp#44,total_xp#45,previous_csr_tier#46,previous_csr_designation#47,previous_csr#48,previous_csr_percent_to_next_tier#49,previous_csr_rank#50,current_csr_tier#51,current_csr_designation#52,current_csr#53,current_csr_percent_to_next_tier#54,current_csr_rank#55,player_rank_on_team#56,player_finished#57,player_average_life#58,player_total_kills#59,player_total_headshots#60,player_total_weapon_damage#61,player_total_shots_landed#62,player_total_melee_kills#63,... 12 more fields] csv\n",
      "      +- RepartitionByExpression [match_id#166], 16\n",
      "         +- Filter isnotnull(match_id#166)\n",
      "            +- Relation [match_id#166,player_gamertag#167,medal_id#168L,count#169] csv\n",
      "\n",
      "== Physical Plan ==\n",
      "AdaptiveSparkPlan isFinalPlan=false\n",
      "+- Sort [mapid#130 ASC NULLS FIRST], false, 0\n",
      "   +- Project [match_id#129, mapid#130, is_team_game#131, playlist_id#132, game_variant_id#133, is_match_over#134, completion_date#135, match_duration#136, game_mode#137, map_variant_id#138, previous_spartan_rank#42, spartan_rank#43, previous_total_xp#44, total_xp#45, previous_csr_tier#46, previous_csr_designation#47, previous_csr#48, previous_csr_percent_to_next_tier#49, previous_csr_rank#50, current_csr_tier#51, current_csr_designation#52, current_csr#53, current_csr_percent_to_next_tier#54, current_csr_rank#55, ... 23 more fields]\n",
      "      +- SortMergeJoin [match_id#129], [match_id#166], Inner\n",
      "         :- Project [match_id#129, mapid#130, is_team_game#131, playlist_id#132, game_variant_id#133, is_match_over#134, completion_date#135, match_duration#136, game_mode#137, map_variant_id#138, previous_spartan_rank#42, spartan_rank#43, previous_total_xp#44, total_xp#45, previous_csr_tier#46, previous_csr_designation#47, previous_csr#48, previous_csr_percent_to_next_tier#49, previous_csr_rank#50, current_csr_tier#51, current_csr_designation#52, current_csr#53, current_csr_percent_to_next_tier#54, current_csr_rank#55, ... 20 more fields]\n",
      "         :  +- SortMergeJoin [match_id#129], [match_id#40], Inner\n",
      "         :     :- Sort [match_id#129 ASC NULLS FIRST], false, 0\n",
      "         :     :  +- Exchange hashpartitioning(match_id#129, 200), REPARTITION_BY_NUM, [plan_id=1910]\n",
      "         :     :     +- Filter isnotnull(match_id#129)\n",
      "         :     :        +- FileScan csv [match_id#129,mapid#130,is_team_game#131,playlist_id#132,game_variant_id#133,is_match_over#134,completion_date#135,match_duration#136,game_mode#137,map_variant_id#138] Batched: false, DataFilters: [isnotnull(match_id#129)], Format: CSV, Location: InMemoryFileIndex(1 paths)[file:/home/iceberg/notebooks/matches.csv], PartitionFilters: [], PushedFilters: [IsNotNull(match_id)], ReadSchema: struct<match_id:string,mapid:string,is_team_game:boolean,playlist_id:string,game_variant_id:strin...\n",
      "         :     +- Sort [match_id#40 ASC NULLS FIRST], false, 0\n",
      "         :        +- Exchange hashpartitioning(match_id#40, 200), REPARTITION_BY_NUM, [plan_id=1911]\n",
      "         :           +- Filter isnotnull(match_id#40)\n",
      "         :              +- FileScan csv [match_id#40,previous_spartan_rank#42,spartan_rank#43,previous_total_xp#44,total_xp#45,previous_csr_tier#46,previous_csr_designation#47,previous_csr#48,previous_csr_percent_to_next_tier#49,previous_csr_rank#50,current_csr_tier#51,current_csr_designation#52,current_csr#53,current_csr_percent_to_next_tier#54,current_csr_rank#55,player_rank_on_team#56,player_finished#57,player_average_life#58,player_total_kills#59,player_total_headshots#60,player_total_weapon_damage#61,player_total_shots_landed#62,player_total_melee_kills#63,player_total_melee_damage#64,... 11 more fields] Batched: false, DataFilters: [isnotnull(match_id#40)], Format: CSV, Location: InMemoryFileIndex(1 paths)[file:/home/iceberg/notebooks/match_details.csv], PartitionFilters: [], PushedFilters: [IsNotNull(match_id)], ReadSchema: struct<match_id:string,previous_spartan_rank:int,spartan_rank:int,previous_total_xp:int,total_xp:...\n",
      "         +- Sort [match_id#166 ASC NULLS FIRST], false, 0\n",
      "            +- Exchange hashpartitioning(match_id#166, 200), REPARTITION_BY_NUM, [plan_id=1918]\n",
      "               +- Filter isnotnull(match_id#166)\n",
      "                  +- FileScan csv [match_id#166,player_gamertag#167,medal_id#168L,count#169] Batched: false, DataFilters: [isnotnull(match_id#166)], Format: CSV, Location: InMemoryFileIndex(1 paths)[file:/home/iceberg/notebooks/medals_matches_players.csv], PartitionFilters: [], PushedFilters: [IsNotNull(match_id)], ReadSchema: struct<match_id:string,player_gamertag:string,medal_id:bigint,count:int>\n",
      "\n"
     ]
    }
   ],
   "source": [
    "from pyspark.sql import functions as F\n",
    "from pyspark.sql import SparkSession\n",
    "spark = SparkSession.builder.appName(\"SortWithinPartitionsTest\").getOrCreate()\n",
    "\n",
    "# SortWithinPartitions by playlist_id\n",
    "sorted_playlist = result.sortWithinPartitions(\"playlist_id\")\n",
    "print(\"Sorting by playlist_id:\")\n",
    "sorted_playlist.explain(True) \n",
    "\n",
    "# SortWithinPartitions by mapid\n",
    "sorted_deaths = result.sortWithinPartitions(\"mapid\")\n",
    "print(\"\\nSorting by map:\")\n",
    "sorted_deaths.explain(True)  "
   ]
  },
  {
   "cell_type": "code",
   "execution_count": null,
   "id": "d47936ae-23be-4e22-859b-7b135c93f154",
   "metadata": {},
   "outputs": [],
   "source": []
  }
 ],
 "metadata": {
  "kernelspec": {
   "display_name": "Python 3 (ipykernel)",
   "language": "python",
   "name": "python3"
  },
  "language_info": {
   "codemirror_mode": {
    "name": "ipython",
    "version": 3
   },
   "file_extension": ".py",
   "mimetype": "text/x-python",
   "name": "python",
   "nbconvert_exporter": "python",
   "pygments_lexer": "ipython3",
   "version": "3.9.18"
  }
 },
 "nbformat": 4,
 "nbformat_minor": 5
}
