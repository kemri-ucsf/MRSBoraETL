drop procedure if exists generate_latest_hiv_summary;
DELIMITER $$
CREATE PROCEDURE generate_latest_hiv_summary()
		BEGIN
    create table if not exists flat_latest_hiv_summary (
	  person_id int,
	  uuid varchar(100),
      encounter_datetime datetime,
      encounter_type varchar (5),
	  location_id int,
	  enrollment_date datetime,
	  hiv_start_date datetime,
      weight int,
	  height int,
	  current_regimen varchar (100),
	  arvs_adherence varchar (5),
      who_stage varchar (5),
      tb_status varchar (5)
      
      );
	insert into flat_latest_hiv_summary(
  select person_id,
       uuid,
       MAX(encounter_datetime) as encounter_datetime,
       encounter_type,
       location_id,
       MAX(enrollment_date) as enrollment_date,
       MAX(hiv_start_date) as hiv_start_date,
       weight,
       height,
       MAX(currentregmen) as currentregmen,
       MAX(arvs_adherence) as arvs_adherence,
       who_stage,
       tb_status
  from flat_visit_summary  
  GROUP BY person_id order by  encounter_datetime desc
);
END $$
	DELIMITER ;

call generate_latest_hiv_summary();