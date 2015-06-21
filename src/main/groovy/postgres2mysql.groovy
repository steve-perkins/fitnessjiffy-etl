@GrabConfig(systemClassLoader=true)
@Grab('org.postgresql:postgresql:9.3-1101-jdbc41')
@Grab('mysql:mysql-connector-java:5.1.35')

import groovy.sql.Sql

def postgres = Sql.newInstance("jdbc:postgresql://localhost/fitnessjiffy", "fitnessjiffy", "fitnessjiffy", "org.postgresql.Driver")
def mysql = Sql.newInstance("jdbc:mysql://localhost/fitnessjiffy", "fitnessjiffy", "fitnessjiffy", "com.mysql.jdbc.Driver")

println "\nPurging any existing data in the target database\n"
mysql.execute("DELETE FROM report_data")
mysql.execute("DELETE FROM exercise_performed")
mysql.execute("DELETE FROM exercise")
mysql.execute("DELETE FROM food_eaten")
mysql.execute("DELETE FROM food")
mysql.execute("DELETE FROM weight")
mysql.execute("DELETE FROM fitnessjiffy_user")

println 'Migrating the "fitnessjiffy_user" table'
postgres.eachRow("""
SELECT id, activity_level, birthdate, created_time, email, first_name, gender, height_in_inches, last_name,
last_updated_time, password_hash, timezone
FROM fitnessjiffy_user
""") {
    mysql.execute("""
INSERT INTO `fitnessjiffy`.`fitnessjiffy_user` (`id`, `activity_level`, `birthdate`, `created_time`, `email`,
`first_name`, `gender`, `height_in_inches`, `last_name`, `last_updated_time`, `password_hash`, `timezone`)
VALUES (${it.id}, ${it.activity_level}, ${it.birthdate}, ${it.created_time}, ${it.email}, ${it.first_name}, ${it.gender},
${it.height_in_inches}, ${it.last_name}, ${it.last_updated_time}, ${it.password_hash}, ${it.timezone})
""")
}

println 'Migrating the "weight" table'
postgres.eachRow("SELECT id, date, pounds, user_id FROM weight") {
    mysql.execute("""
INSERT INTO `fitnessjiffy`.`weight` (`id`, `date`, `pounds`, `user_id`)
VALUES (${it.id}, ${it.date}, ${it.pounds}, ${it.user_id})
""")
}

println 'Migrating the "exercise" table'
postgres.eachRow("SELECT id, category, code, description, metabolic_equivalent FROM exercise") {
    mysql.execute("""
INSERT INTO `fitnessjiffy`.`exercise` (`id`, `category`, `code`, `description`, `metabolic_equivalent`)
VALUES (${it.id}, ${it.category}, ${it.code}, ${it.description}, ${it.metabolic_equivalent})
""")
}

println 'Migrating the "exercise_performed" table'
postgres.eachRow("SELECT id, date, minutes, exercise_id, user_id FROM exercise_performed") {
    mysql.execute("""
INSERT INTO `fitnessjiffy`.`exercise_performed` (`id`, `date`, `minutes`, `exercise_id`, `user_id`)
VALUES (${it.id}, ${it.date}, ${it.minutes}, ${it.exercise_id}, ${it.user_id})
""")
}

println 'Migrating the "food" table'
postgres.eachRow("""
SELECT id, calories, carbs, created_time, default_serving_type, fat, fiber, last_updated_time, name, protein,
saturated_fat, serving_type_qty, sodium, sugar, owner_id
FROM food
""") {
    mysql.execute("""
INSERT INTO `fitnessjiffy`.`food` (`id`, `calories`, `carbs`, `created_time`, `default_serving_type`, `fat`, `fiber`,
`last_updated_time`, `name`, `protein`, `saturated_fat`, `serving_type_qty`, `sodium`, `sugar`, `owner_id`)
VALUES (${it.id}, ${it.calories}, ${it.carbs}, ${it.created_time}, ${it.default_serving_type}, ${it.fat}, ${it.fiber},
${it.last_updated_time}, ${it.name}, ${it.protein}, ${it.saturated_fat}, ${it.serving_type_qty}, ${it.sodium},
${it.sugar}, ${it.owner_id})
""")
}

println 'Migrating the "food_eaten" table'
postgres.eachRow("SELECT id, date, serving_qty, serving_type, food_id, user_id FROM food_eaten") {
    mysql.execute("""
INSERT INTO `fitnessjiffy`.`food_eaten` (`id`, `date`, `serving_qty`, `serving_type`, `food_id`, `user_id`)
VALUES (${it.id}, ${it.date}, ${it.serving_qty}, ${it.serving_type}, ${it.food_id}, ${it.user_id})
""")
}

println 'Migrating the "report_data" table'
postgres.eachRow("SELECT id, date, net_calories, net_points, pounds, user_id FROM report_data") {
    mysql.execute("""
INSERT INTO `fitnessjiffy`.`report_data` (`id`, `date`, `net_calories`, `net_points`, `pounds`, `user_id`)
VALUES (${it.id}, ${it.date}, ${it.net_calories}, ${it.net_points}, ${it.pounds}, ${it.user_id})
""")
}

println "\nComplete!\n"

