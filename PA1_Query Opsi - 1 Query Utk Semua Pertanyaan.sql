-- Info Kolom --
-- Arti 1. consecutive_number (integer) = NOMOR URUT -- ID unik dari setiap kecelakaan = 35450
-- Arti 2. state_name (character varying) = NAMA NEGARA BAGIAN -- Nama Negara Bagian = 51
-- Arti 3. number_of_vehicle_forms_submitted_all (integer) = JUMLAH KENDARAAN YANG TERDAFTAR -- yang terlibat kecelakaan
-- Arti 4. number_of_motor_vehicles_in_transport_mvit (integer) = JUMLAH KENDARAAN BERMOTOR -- jumlah kendaraan yang sedang melintas dan kecelakaan
-- Arti 5. number_of_parked_working_vehicles (integer) = JUMLAH KENDARAAN DINAS YANG DIPARKIR -- 
-- Arti 6. number_of_forms_submitted_for_persons_not_in_motor_vehicles (integer) = JUMLAH ORANG YANG TERDAFTAR PADA KECELAKAAN DAN TIDAK MENGGUNAKAN KENDARAAN BERMOTOR --
-- Arti 7. number_of_persons_in_motor_vehicles_in_transport_mvit (integer) = JUMLAH ORANG DALAM KENDARAAN BERMOTOR YANG MENGALAMI KECELAKAAN --
-- Arti 8. number_of_persons_not_in_motor_vehicles_in_transport_mvit (integer) = JUMLAH ORANG YANG TIDAK MENGGUNAKAN KENDARAAN BERMOTOR --
-- Arti 9. city_name (character varying) = NAMA KOTA --
-- Arti 10. land_use_name (character varying) = NAMA PENGGUNAAN LAHAN --
-- Arti 11. functional_system_name (character varying) = NAMA SISTEM FUNGSIONAL --
-- Arti 12. milepoint (integer) = JARAK KM / MIL --
-- Arti 13. manner_of_collision_name (character varying) = NAMA JENIS KECELAKAAN / TABRAKAN --
-- Arti 14. type_of_intersection_name (character varying) = NAMA JENIS PERSIMPANGAN --
-- Arti 15. light_condition_name (character varying) = NAMA KONDISI CAHAYA --
-- Arti 16. atmospheric_conditions_1_name (character varying) = NAMA KONDISI ATMOSFIR --
-- Arti 17. number_of_fatalities (integer) = JUMLAH KORBAN JIWA --
-- Arti 18. number_of_drunk_drivers (integer) = JUMLAH PENGEMUDI MABUK --
-- Arti 19. timestamp_of_crash (timestamp with time zone) = WAKTU KECELAKAAN --
with crashCL as(
	select
		consecutive_number as crash_id,
		state_name,
		number_of_vehicle_forms_submitted_all,
		number_of_motor_vehicles_in_transport_mvit,
		number_of_parked_working_vehicles,
		number_of_forms_submitted_for_persons_not_in_motor_vehicles,
		number_of_persons_in_motor_vehicles_in_transport_mvit,
		number_of_persons_not_in_motor_vehicles_in_transport_mvit,
		city_name,
		CASE 
			WHEN land_use_name not in ('Rural','Urban')
			THEN 'Others' 
			ELSE land_use_name 
		END,
		CASE 
			WHEN functional_system_name in ('Unknown','Not Reported','Trafficway Not in State Inventory')
			THEN 'Óthers'
			ELSE functional_system_name 
		END,
		milepoint,
		CASE
			WHEN manner_of_collision_name in ('Not Reported','Repoted as Unknown')
			THEN 'Other'
			ELSE manner_of_collision_name 
		END,
		CASE
			WHEN type_of_intersection_name in ('Not Reported','Reported as Unknown')
			THEN 'Other Intersection Type' 
			ELSE type_of_intersection_name
		END,
		CASE 
			WHEN light_condition_name in ('Not Reported','Reported as Unknown')
			THEN 'Other'
			WHEN light_condition_name = 'Dark - Unknown Lighting'
			THEN 'Dark - Not Lighted'
			ELSE light_condition_name
		END,
		CASE
			WHEN atmospheric_conditions_1_name in ('Not Reported','Reported as Unknown')
			THEN 'Other'
			ELSE atmospheric_conditions_1_name
		END,
		number_of_fatalities,
		number_of_drunk_drivers,
		timestamp_of_crash,
		CASE
			WHEN state_name in ('Oklahoma','Mississippi','Louisiana','Arkansas',
								'Missouri','South Dakota','Iowa','Minnesota','Wisconsin',
								'Illinois','North Dakota','Nebraska','Kansas','Texas','Alabama')
			THEN timestamp_of_crash AT TIME ZONE 'CST'
			WHEN state_name in ('North Carolina','Florida','Vermont','Delaware','New York',
								'West Virginia','South Carolina','New Jersey','Connecticut',
								'District of Columbia','Indiana','Massachusetts',
								'Rhode Island','Ohio','Michigan','Pennsylvania','Kentucky',
								'Virginia','Maryland','Georgia','New Hampshire','Maine','Tennessee')
			THEN timestamp_of_crash AT TIME ZONE 'EST'
			WHEN state_name in ('Colorado','New Mexico','Montana','Arizona','Utah','Wyoming','Oregon','Idaho')
			THEN timestamp_of_crash AT TIME ZONE 'MST'
			WHEN state_name in ('Nevada','Washington','California')
			THEN timestamp_of_crash AT TIME ZONE 'MST'
			WHEN state_name in ('Hawaii')
			THEN timestamp_of_crash AT TIME ZONE 'HST'
			WHEN state_name in ('Alaska')
			THEN timestamp_of_crash AT TIME ZONE 'AKST'
		END origin_timezone
	from crash where
		CASE
			WHEN state_name in ('Oklahoma','Mississippi','Louisiana','Arkansas',
								'Missouri','South Dakota','Iowa','Minnesota','Wisconsin',
								'Illinois','North Dakota','Nebraska','Kansas','Texas','Alabama')
			THEN timestamp_of_crash AT TIME ZONE 'CST'
			WHEN state_name in ('North Carolina','Florida','Vermont','Delaware','New York',
								'West Virginia','South Carolina','New Jersey','Connecticut',
								'District of Columbia','Indiana','Massachusetts',
								'Rhode Island','Ohio','Michigan','Pennsylvania','Kentucky',
								'Virginia','Maryland','Georgia','New Hampshire','Maine','Tennessee')
			THEN timestamp_of_crash AT TIME ZONE 'EST'
			WHEN state_name in ('Colorado','New Mexico','Montana','Arizona','Utah','Wyoming','Oregon','Idaho')
			THEN timestamp_of_crash AT TIME ZONE 'MST'
			WHEN state_name in ('Nevada','Washington','California')
			THEN timestamp_of_crash AT TIME ZONE 'MST'
			WHEN state_name in ('Hawaii')
			THEN timestamp_of_crash AT TIME ZONE 'HST'
			WHEN state_name in ('Alaska')
			THEN timestamp_of_crash AT TIME ZONE 'AKST'
		END between '2020-12-31 23:59:59' and '2022-1-1'
),
number_one1 as (
	select
		ROW_NUMBER()over(order by count(*) desc) nomor_urut,
 		light_condition_name,ROUND((count(*)/sum(count(*)) over())*100,2) presentase 
 	from crashCL group by light_condition_name order By count(*) desc
),
number_one2 as (
	select
		ROW_NUMBER()over(order by count(*) desc) nomor_urut,
 		atmospheric_conditions_1_name,ROUND((count(*)/sum(count(*)) over())*100,2) presentase 
 	from crashCL group by atmospheric_conditions_1_name order By count(*) desc
),
number_one3 as (
	select
		ROW_NUMBER()over(order by count(*) desc) nomor_urut,
 		type_of_intersection_name,ROUND((count(*)/sum(count(*)) over())*100,2) presentase
 	from crashCL group by type_of_intersection_name order By count(*) desc
),
number_one4 as (
	select
		ROW_NUMBER()over(order by count(*) desc) nomor_urut,
 		CASE
			WHEN number_of_drunk_drivers>0
			THEN 'Drunk'
			ELSE 'No_Drunk'
		END drunk_drivers,
		ROUND((count(*)/sum(count(*)) over())*100,2) presentase 
 	from crashCL group by drunk_drivers order By count(*) desc
),
number_two as(
	select state_name,count(*) from crashCL group by state_name order by count(*) desc
),
number_three as(
	select
		distinct extract(HOUR from origin_timezone) waktu,
		count(*)/365
	from crashCL group by waktu order by waktu
),
number_five as (
	select
		ROW_NUMBER()over(order by count(*) desc) nomor_urut,
 		land_use_name,ROUND((count(*)/sum(count(*)) over())*100,2) presentase
 	from crashCL group by land_use_name order By count(*) desc
),
number_six as(
	select
		distinct to_char(origin_timezone,'D. Day') hari,
		count(*)
	from crashCL group by hari order by hari
)
/*select * from number_one1 right join number_one2
	on number_one1.nomor_urut = number_one2.nomor_urut
	full join number_one3 on number_one2.nomor_urut = number_one3.nomor_urut
	full join number_one4 on number_one2.nomor_urut = number_one4.nomor_urut*/
	
/*select * from number_two limit 10

/*select * from number_three */

/*select * from number_one4*/

/*select * from number_five*/

/*select * from number_six*/
