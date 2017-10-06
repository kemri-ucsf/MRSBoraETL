use etl;
drop procedure if exists generate_visit_summary;
 DELIMITER $$
 CREATE PROCEDURE generate_visit_summary()
 		BEGIN
        select @query_type := "sync"; # this can be either sync or rebuild
					select @start := now();
					select @start := now();
					select @table_version := "flat_visit_summary_v1.0";

					set session sort_buffer_size=512000000;

					select @sep := " ## ";
					select @lab_encounter_type := 99999;
					select @death_encounter_type := 31;
					select @last_date_created := (select max(max_date_created) from etl.flat_obs);
                    create table if not exists flat_visit_summary (
						person_id int,
						uuid varchar(100),
						visit_id int,
					    encounter_id int,
						encounter_datetime datetime,
						encounter_type int,
						is_clinical_encounter int,
						location_id int,
						enrollment_date datetime,
						hiv_start_date datetime,
                        patient_source varchar(50),
                        weight varchar(5),
                        height varchar(5),
                        cur_arv_adherence varchar(10),
                        pregnant varchar(100),
                        fp_status varchar(5),
						fp_method varchar(100),
                        why_not_on_fp varchar(100),
                        tb_status varchar(100),
                        side_effects varchar(100),
                        who_stage varchar(100),
                        ctx_adherence varchar(5),
                        ctx_dispensed varchar(15),
                        inh_dispensed varchar(5),
                        other_medications_dispensed varchar(5),
                        arvs_adherence varchar(10),
                        why_arvs_adherence_poor varchar(100),
                        pmtct_ppct varchar(10),
                        currentregmen varchar (50),
                        cd4_cd4percentage_done varchar(3),
                        hgb_done varchar(3),
                        rpr_done varchar(3),
                        tb_sputum_done varchar(3),
                        other_test_done varchar(3),
                        refferal_hospitalized varchar(60),
                        no_of_day_hospitalized varchar(2),
                        breastfing_mode varchar(50),
                        at_risk_population varchar(50),
                        pwp_disclosure varchar(3),
                        pwp_partner_tested varchar(3),
                        condoms_dispensed varchar(3),
					    sti_screened varchar(3),
					    tca datetime,
                        primary key encounter_id (encounter_id),
                        index person_date (person_id, encounter_datetime),
						index person_uuid (uuid),
						-- index location_enc_date (location_uuid,encounter_datetime),
						-- index enc_date_location (encounter_datetime, location_uuid),
						index encounter_type (encounter_type)
					);
                    select @last_update := (select max(date_updated) from etl.flat_log where table_name=@table_version);
                   # then use the max_date_created from openmrs.encounter. This takes about 10 seconds and is better to avoid.
					select @last_update :=
						if(@last_update is null,
							(select max(date_created) from openmrs.encounter e join etl.flat_visit_summary using (encounter_id)),
							@last_update);
                            #otherwise set to a date before any encounters had been created (i.g. we will get all encounters)
					select @last_update := if(@last_update,@last_update,'1900-01-01');
					#select @last_update := "2016-09-12"; #date(now());
					#select @last_date_created := "2015-11-17"; #date(now());
                    	# drop table if exists flat_hiv_summary_queue;
					create  table if not exists flat_visit_summary_queue(person_id int, primary key (person_id));
                    
                    # we will add new patient id to be rebuilt when either we  are in sync mode or if the existing table is empty
					# this will allow us to restart rebuilding the table if it crashes in the middle of a rebuild
					select @num_ids := (select count(*) from flat_visit_summary_queue limit 1);

					if (@num_ids=0 or @query_type="sync") then
                        replace into flat_visit_summary_queue
                        (select distinct patient_id #, min(encounter_datetime) as start_date
                            from openmrs.encounter
                            where date_changed > @last_update
                        );


                        replace into flat_visit_summary_queue
                        (select distinct person_id #, min(encounter_datetime) as start_date
                            from etl.flat_obs
                            where max_date_created > @last_update
                        #	group by person_id
                        # limit 10
                        );
                      # Lab encountres
                      /*  replace into flat_hiv_summary_queue
                        (select distinct person_id
                            from etl.flat_lab_obs
                            where max_date_created > @last_update
                        ); */
                     # Lab Orders
                       /* replace into flat_hiv_summary_queue
                        (select distinct person_id
                            from etl.flat_orders
                            where max_date_created > @last_update
                        );
                        */
                         end if;
                         
                        select @person_ids_count := (select count(*) from flat_visit_summary_queue);

				   delete t1 from flat_visit_summary t1 join flat_visit_summary_queue t2 using (person_id);

					while @person_ids_count > 0 do

						#create temp table with a set of person ids
						drop table if exists flat_visit_summary_queue_0;

						create temporary table flat_visit_summary_queue_0 (select * from flat_visit_summary_queue limit 5000); #TODO - change this when data_fetch_size changes


						select @person_ids_count := (select count(*) from flat_visit_summary_queue);

						drop table if exists flat_visit_summary_0a;
						create temporary table flat_visit_summary_0a
						(select
							t1.person_id,
							t1.visit_id,
							t1.encounter_id,
							t1.encounter_datetime,
							t1.encounter_type,
							t1.location_id,
							t1.obs,
							t1.obs_datetimes,
                            
                            case
								when t1.encounter_type in (21,22) then 1
								else null
							end as is_clinical_encounter,
                            case
						        when t1.encounter_type in (35) then 20
								when t1.encounter_type in (45) then 10
								else 1
							end as encounter_type_sort_index,
                            t2.orders
							from etl.flat_obs t1
								join flat_visit_summary_queue_0 t0 using (person_id)
								left join etl.flat_orders t2 using(encounter_id)
						#		join flat_hiv_summary_queue t0 on t1.person_id=t0.person_id and t1.encounter_datetime >= t0.start_date
							where t1.encounter_type in (21,22)
						);
                        insert into flat_visit_summary_0a
						(select
							t1.person_id,
							null,
							t1.encounter_id,
							t1.test_datetime,
							t1.encounter_type,
							t1.location_id, # null, ,
							t1.obs,
							null, #obs_datetimes
							# in any visit, there many be multiple encounters. for this dataset, we want to include only clinical encounters (e.g. not lab or triage visit)
							0 as is_clinical_encounter,
							1 as encounter_type_sort_index,
							null
							from etl.flat_lab_obs t1
								join flat_visit_summary_queue_0 t0 using (person_id)
						);
                        
                        drop table if exists flat_visit_summary_0;
						create temporary table flat_visit_summary_0(index encounter_id (encounter_id), index person_enc (person_id,encounter_datetime))
						(select * from flat_visit_summary_0a
						order by person_id, date(encounter_datetime), encounter_type_sort_index
						);
                        
                       
						select @prev_id := null;
						select @cur_id := null;
                        select @enrollment_date := null;
						select @hiv_start_date := null;
                        drop temporary table if exists flat_visit_summary_1;
						create temporary table flat_visit_summary_1 (index encounter_id (encounter_id))
						(select
							encounter_type_sort_index,
							@prev_id := @cur_id as prev_id,
							@cur_id := t1.person_id as cur_id,
							t1.person_id,
							p.uuid,
							t1.visit_id,
							t1.encounter_id,
							t1.encounter_datetime,
							t1.encounter_type,
							t1.is_clinical_encounter,
							t1.location_id,
                             #weight
                            case
								 when obs regexp "!!6743=" then @weight:= replace(replace((substring_index(substring(obs,locate("!!6743=",obs)),@sep,ROUND ((LENGTH(obs) - LENGTH( REPLACE ( obs, "!!6743=", "") ) ) / LENGTH("!!6743=") ))),"!!6743=",""),"!!","")
                                 else @weight:= null
							end as weight,
                            #height
                            case
								 when obs regexp "!!6744=" then @height:= replace(replace((substring_index(substring(obs,locate("!!6744=",obs)),@sep,ROUND ((LENGTH(obs) - LENGTH( REPLACE ( obs, "!!6744=", "") ) ) / LENGTH("!!6744=") ))),"!!6744=",""),"!!","")
                                 else @height:= null
							end as height,
                            # Pregnancy
                            case
								when obs regexp "!!1836=(1065)!!" then @pregnancy:="Yes"
								when obs regexp "!!1836=(1066)!!" then @pregnancy:="No"
                                when obs regexp "!!1836=(6765)!!" then @pregnancy:="MC-Recently Miscarried"
                                when obs regexp "!!1836=(50)!!" then @pregnancy:="AB-Recently Induced Abortion"
                                when obs regexp "!!1836=(6847)!!" then @pregnancy:="Live Birth"
                                when obs regexp "!!1836=(6848)!!" then @pregnancy:="Still Birth"
								else @pregnancy:= null
							end as pregnancy
                            ,
                             # FP Status
                            case
								when obs regexp "!!5271=(1065)!!" then @fpstatus:="FP"
								when obs regexp "!!5271=(1066)!!" then @fpstatus:="No FP"
                                when obs regexp "!!5271=(6765)!!" then @fpstatus:="WFP"
								else @fpstatus:= null
							end as fpstatus
                            ,
							 # FP Method
                            case
								when obs regexp "!!374=(190)!!" then @fpmethod:="Condoms (C)"
								when obs regexp "!!374=(6495)!!" then @fpmethod:="Emergency Contraceptive (ECP)"
                                when obs regexp "!!374=(780)!!" then @fpmethod:="Oral Contraceptive Pills (OC)"
                                when obs regexp "!!374=(5279)!!" then @fpmethod:="Injectables (INJ)"
                                when obs regexp "!!374=(1713)!!" then @fpmethod:="Implant (IMP)"
                                when obs regexp "!!374=(5275)!!" then @fpmethod:="Intrauterine Devices (IUD)"
                                when obs regexp "!!374=(6496)!!" then @fpmethod:="Lactational Amenorrhea (LAM)"
                                when obs regexp "!!374=(5278)!!" then @fpmethod:="Diaphragm/Cervical Cap (D)"
                                when obs regexp "!!374=(6783)!!" then @fpmethod:="Fertility Awareness(FA)"
                                when obs regexp "!!374=(5276)!!" then @fpmethod:="Female Sterilization (BTL/TL)"
                                when obs regexp "!!374=(1771)!!" then @fpmethod:="Vasectomy (V)"
								else @fpmethod:= null
							end as fpmethod
                            ,
                            # Why Not FP
                            case
								when obs regexp "!!6758=(1447)!!" then @why_not_on_fp:="Wants to get pregnant (WP)"
								when obs regexp "!!6758=(6757)!!" then @why_not_on_fp:="Thinks can't get pregnant (UP)"
                                when obs regexp "!!6758=(6765)!!" then @why_not_on_fp:="Not sexually active now (NSex)"
                                when obs regexp "!!6758=(6368)!!" then @why_not_on_fp:="Pregnant"
                                when obs regexp "!!6758=(6854)!!" then @why_not_on_fp:="Abdominal hysterectomy(TAH)"
                                when obs regexp "!!6758=(6825)!!" then @why_not_on_fp:="Undecided(UND)"
								else @why_not_on_fp:= null
							end as why_not_on_fp
                            ,
                            # Tuberculosis Status
                            case
								when obs regexp "!!6150=(1066)!!" then @tb_status:="NO Signs"
								when obs regexp "!!6150=(6182)!!" then @tb_status:="TB Suspected"
                                when obs regexp "!!6150=(6183)!!" then @tb_status:="TB Rx"
                                when obs regexp "!!6150=(6151)!!" then @tb_status:="Not done (ND)"
                                else @tb_status:= null
							end as tb_status
                            ,
                            # Potential Side Effects
                             case
								when obs regexp "!!6175=(1107)!!" then @side_effects:="None"
								when obs regexp "!!6175=(5978)!!" then @side_effects:="Nausea (N)"
                                when obs regexp "!!6175=(512)!!" then @side_effects:="Rash (R)"
                                when obs regexp "!!6175=(620)!!" then @side_effects:="Headache (H)"
                                when obs regexp "!!6175=(16)!!" then @side_effects:="Diarrhoea (D)"
                                when obs regexp "!!6175=(3)!!" then @side_effects:="Anaemia (A)"
                                else @side_effects:= null
							end as side_effects
                            ,
                       
							case
								when obs regexp "!!6746=" then @enrollment_date :=
									replace(replace((substring_index(substring(obs,locate("!!6746=",obs)),@sep,ROUND ((LENGTH(obs) - LENGTH( REPLACE ( obs, "!!6746=", "") ) ) / LENGTH("!!6746=") ))),"!!6746=",""),"!!","")
								else @enrollment_date:= null
							end as enrollment_date,
                            #patient source
                            case
								 when obs regexp "!!1353=(1356)!!" then @patient_source:="PMTCT"
								 when obs regexp "!!1353=(1354)!!" then @patient_source:="VCT"
                                 when obs regexp "!!1353=(6767)!!" then @patient_source:="IPD-Ad"
								 when obs regexp "!!1353=(1360)!!" then @patient_source:="TB Clinic"
                                 when obs regexp "!!1353=(1357)!!" then @patient_source:="OPD"
								 when obs regexp "!!1353=(6768)!!" then @patient_source:="IPD-Ch"
                                 when obs regexp "!!1353=(1358)!!" then @patient_source:="MCH-Child"
								 when obs regexp "!!1353=(1828)!!" then @patient_source:="VMMC"
                                 when obs regexp "!!1353=(1355)!!" then @patient_source:="Family Member"
                                 when obs regexp "!!1353=(5622)!!" then @patient_source:="Other"
                                 else @patient_source:= replace(replace((substring_index(substring(obs,locate("!!1353=",obs)),@sep,ROUND ((LENGTH(obs) - LENGTH( REPLACE ( obs, "!!1353=", "") ) ) / LENGTH("!!1353=") ))),"!!1353=",""),"!!","")

							end as patient_source,
                             
                            # 6760 ART ADHERENCE
                            # 1384 = GOOD
							# 1385 = FAIR
							# 1386 = POOR
                            # 1175 = N/A
							case
								when obs regexp "!!6760=1384!!" then @cur_arv_adherence := 'GOOD'
								when obs regexp "!!6760=1385!!" then @cur_arv_adherence := 'FAIR'
								when obs regexp "!!6760=1386!!" then @cur_arv_adherence := 'POOR'
                                when obs regexp "!!6760=1175!!" then @cur_arv_adherence := 'N/A'
								else @cur_arv_adherence := null
							end as cur_arv_adherence,
                            
                             # WHO Stage
                             case
								when obs regexp "!!6794=(6790)!!" then @who_stage:="1"
								when obs regexp "!!6794=(6790)!!" then @who_stage:="2"
                                when obs regexp "!!6794=(6790)!!" then @who_stage:="3"
                                when obs regexp "!!6794=(6790)!!" then @who_stage:="4"
                                when obs regexp "!!6794=(1852)!!" then @who_stage:="Not Stage"
                                else @who_stage:= null
							end as who_stage,
                            # Cotrimoxazole Adherence
                            case
								when obs regexp "!!6761=(1384)!!" then @ctx_adherence:="Good"
								when obs regexp "!!6761=(1385)!!" then @ctx_adherence:="Fair"
                                when obs regexp "!!6761=(1386)!!" then @ctx_adherence:="Poor"
                                when obs regexp "!!6761=(1175)!!" then @ctx_adherence:="N/A"
                                else @ctx_adherence:= null
							end as ctx_adherence,
                            #ctx Dispensed
                            case
								when obs regexp "!!1565=(1427)!!" then @ctx_dispensed:="Yes"
								when obs regexp "!!1565=(1066)!!" then @ctx_dispensed:="No"
                                when obs regexp "!!1565=(92)!!" then @ctx_dispensed:="Dapson"
								else @ctx_dispensed:= null
							end as ctx_dispensed,
                            
                            #inh_dispensed
                             case
								when obs regexp "!!6785=(1065)!!" then @inh_dispensed:="Yes"
								when obs regexp "!!6785=(1066)!!" then @inh_dispensed:="No"
                                when obs regexp "!!6785=(1175)!!" then @inh_dispensed:="N/A"
								else @inh_dispensed:= null
							end as inh_dispensed,
                            # Other Medications Dispensed
                            case
								when obs regexp "!!6784=(1065)!!" then @other_medications_dispensed:="Yes"
								when obs regexp "!!6784=(1066)!!" then @other_medications_dispensed:="No"
                                else @other_medications_dispensed:= null
							end as other_medications_dispensed,
                            # ARVs Adherence
                            case
								when obs regexp "!!6760=(1384)!!" then @arvs_adherence:="Good"
								when obs regexp "!!6760=(1385)!!" then @arvs_adherence:="Fair"
                                when obs regexp "!!6760=(1386)!!" then @arvs_adherence:="Poor"
                                when obs regexp "!!6760=(1175)!!" then @arvs_adherence:="N/A"
                                when obs regexp "!!6760=(8015)!!" then @arvs_adherence:="Stopped"
                                else @arvs_adherence:= null
							end as arvs_adherence,
                            # why_arvs_adherence_poor
                               case
                                    when obs regexp "!!6171=(1413)!!" then @why_arvs_adherence_poor:="Toxicity/Side effects"  
									when obs regexp "!!6171=(6169)!!" then @why_arvs_adherence_poor:="Share with others"  
									when obs regexp "!!6171=(1410)!!" then @why_arvs_adherence_poor:="Forgot"  
									when obs regexp "!!6171=(6168)!!" then @why_arvs_adherence_poor:="Felt better"  
									when obs regexp "!!6171=(1415)!!" then @why_arvs_adherence_poor:="Felt too ill"    
									when obs regexp "!!6171=(1411)!!" then @why_arvs_adherence_poor:="Stigma, disclosure or privacy issues"   
									when obs regexp "!!6171=(1417)!!" then @why_arvs_adherence_poor:="Drug stock out" 
									when obs regexp "!!6171=(1414)!!" then @why_arvs_adherence_poor:="Patient lost/run out of pills"  
									when obs regexp "!!6171=(820)!!"  then @why_arvs_adherence_poor:="Delivery/transport problems"  
									when obs regexp "!!6171=(6170)!!" then @why_arvs_adherence_poor:="Inability to pay"  
									when obs regexp "!!6171=(1624)!!" then @why_arvs_adherence_poor:="Alcohol"  
									when obs regexp "!!6171=(207)!!"  then @why_arvs_adherence_poor:="Depression"     
									when obs regexp "!!6171=(6202)!!" then @why_arvs_adherence_poor:="Pill burden"   
									when obs regexp "!!6171=(5622)!!" then @why_arvs_adherence_poor:="Other (Specify)"
                                else @why_arvs_adherence_poor:= null
							end as why_arvs_adherence_poor,
                            # pmtct_ppct
                            case
								when obs regexp "!!1592=(1405)!!" then @pmtct_ppct:="PMTCT/PPCT"
								else @pmtct_ppct:= null
							end as pmtct_ppct,
                            #currentregmen
                            case
								when obs regexp "!!1571=(628)!!"  then @currentregmen:="3TC"
								when obs regexp "!!1571=(814)!!"  then @currentregmen:="ABC"
								when obs regexp "!!1571=(6285)!!" then @currentregmen:="AF1A/CF1A-3TC/AZT/NVP"
								when obs regexp "!!1571=(6286)!!" then @currentregmen:="AF1B/CF1B-3TC/AZT/EFV"
								when obs regexp "!!1571=(6288)!!" then @currentregmen:="AF2A-3TC/NVP/TDF"
								when obs regexp "!!1571=(6289)!!" then @currentregmen:="AF2B-3TC/EFV/TDF"
								when obs regexp "!!1571=(6284)!!" then @currentregmen:="AF3A/CF3A-3TC/d4t/NVP"
								when obs regexp "!!1571=(6287)!!" then @currentregmen:="AF3B/CF3B-3TC/d4t/EFV"
								when obs regexp "!!1571=(6290)!!" then @currentregmen:="AO1A/CF2A-3TC/ABC/NVP"
								when obs regexp "!!1571=(6291)!!" then @currentregmen:="AO1B/CF2B-3TC/ABC/EFV"
								when obs regexp "!!1571=(6297)!!" then @currentregmen:="AO1C/CF2D-3TC/ABC/LPV"
								when obs regexp "!!1571=(6293)!!" then @currentregmen:="AS1A/CF1C/CS1A-3TC/AZT/LPV/r"
								when obs regexp "!!1571=(6298)!!" then @currentregmen:="AS1C/CF2C-3TC/ABC/AZT"
								when obs regexp "!!1571=(6296)!!" then @currentregmen:="AS2A-3TC/LPV/r/TDF"
								when obs regexp "!!1571=(6292)!!" then @currentregmen:="AS2D-ABC/LPV/r/TDF"
								when obs regexp "!!1571=(6294)!!" then @currentregmen:="AS4A/CS3A-3TC/d4t/LPV/r"
								when obs regexp "!!1571=(797)!!"  then @currentregmen:="AZT-ZIDOVUDINE"
								when obs regexp "!!1571=(6299)!!" then @currentregmen:="CS1B-ABC/AZT/LPV/r"
								when obs regexp "!!1571=(6297)!!" then @currentregmen:="CS2A-3TC/ABC/LPV/r"
								when obs regexp "!!1571=(625)!!"  then @currentregmen:="d4t-STAVUDINE"
								when obs regexp "!!1571=(633)!!"  then @currentregmen:="AFV-EFAVIRENZ"
								when obs regexp "!!1571=(794)!!"  then @currentregmen:="LPV/r-LOPINAVIR AND RITONAVIR"
								when obs regexp "!!1571=(635)!!"  then @currentregmen:="NVF-NELFINAVIR"
								when obs regexp "!!1571=(631)!!"  then @currentregmen:="NVP-NEVIRAPINE"
								when obs regexp "!!1571=(802)!!"  then @currentregmen:="TDF-TENOFOVIR"
								when obs regexp "!!1571=(5424)!!" then @currentregmen:="Other-OTHER ANTIRETROVIRAL DRUG"
								when obs regexp "!!1571=(7500)!!" then @currentregmen:="AS2C-TDF/3TC/ATV/r"
								when obs regexp "!!1571=(7501)!!" then @currentregmen:="AS1B-AZT-3TC-ATV/r"
								when obs regexp "!!1571=(7499)!!" then @currentregmen:="ATV/r - Atazanavir/Ritonavir"
								when obs regexp "!!1571=(7621)!!" then @currentregmen:="AS5B-ABC/3TC/ATV/r"
								when obs regexp "!!1571=(7626)!!" then @currentregmen:="AS6X-D4T/3TC/ATV/r"
								when obs regexp "!!1571=(7642)!!" then @currentregmen:="TDF/3TC/LPV/r-CF4C"
								when obs regexp "!!1571=(7644)!!" then @currentregmen:="TDF/3TC/ATV/r-CF4D"
								when obs regexp "!!1571=(7643)!!" then @currentregmen:="TDF/3TC/LPV/r-CS4X"
								when obs regexp "!!1571=(7645)!!" then @currentregmen:="TDF/3TC/ATV/r-CS4X"
								when obs regexp "!!1571=(7649)!!" then @currentregmen:="ABC/3TC/LPV/r-AS5A"
								when obs regexp "!!1571=(7652)!!" then @currentregmen:="AZT/TDF/EFV"
								when obs regexp "!!1571=(7653)!!" then @currentregmen:="ABC/3TC/EFV-AF4B"
								when obs regexp "!!1571=(7654)!!" then @currentregmen:="ABC/3TC/NVP-AF4A"
								else @currentregmen:= null
							end as currentregmen,
                            # cd4_cd4percentage
                            case
								when obs regexp "!!1271=(5497)!!" then @cd4_cd4percentage:="Yes"
								else @cd4_cd4percentage:= "No"
							end as cd4_cd4percentage,
                            
                            # Hgb
                            case
								when obs regexp "!!1271=(21)!!" then @Hgb:="Yes"
								else @Hgb:= "No"
							end as Hgb,
                           # rpr_done
                            case
								when obs regexp "!!1271=(1569)!!" then @rpr_done:="Yes"
								else @rpr_done:= "No"
							end as rpr_done,
                            # tb_sputum_done
                            case
								when obs regexp "!!1271=(1883)!!" then @tb_sputum_done:="Yes"
								else @tb_sputum_done:= "No"
							end as tb_sputum_done,
                            # other_test_done
                            case
								when obs regexp "!!1271=(5622)!!" then @other_test_done:="Yes"
								else @other_test_done:= "No"
							end as other_test_done,
                            
                            #refferal_hospitalized
                            case
								when obs regexp "!!1272=(1107)!!" then @refferal_hospitalized:="None"
								when obs regexp "!!1272=(5488)!!" then @refferal_hospitalized:="Adherence counseling (AD)"
								when obs regexp "!!1272=(1356)!!" then @refferal_hospitalized:="ANC/PMTCT"
								when obs regexp "!!1272=(1167)!!" then @refferal_hospitalized:="Disclosure counseling (DC)"
								when obs regexp "!!1272=(5483)!!" then @refferal_hospitalized:="Family planning (FP)"
								when obs regexp "!!1272=(5485)!!" then @refferal_hospitalized:="Inpatient care/hospitalization"
								when obs regexp "!!1272=(5484)!!" then @refferal_hospitalized:="Nutritional services (NS)"
								when obs regexp "!!1272=(5486)!!" then @refferal_hospitalized:="Social support group (SSG)"
								when obs regexp "!!1272=(5490)!!" then @refferal_hospitalized:="Psychosocial counseling (PC)"
								when obs regexp "!!1272=(5487)!!" then @refferal_hospitalized:="TB treatment/Dot program (TB)"
								when obs regexp "!!1272=(1167)!!" then @refferal_hospitalized:="Other (specify)"
								else @refferal_hospitalized:= null
							end as refferal_hospitalized,
                            # no_of_day_hospitalized
                            case
								when obs regexp "!!1534=" then @no_of_day_hospitalized:=(replace(replace((substring_index(substring(obs,locate("!!(1534)=",obs)),@sep,1)),"!!(1534)=",""),"!!","")) 
								else @no_of_day_hospitalized:= null
							end as no_of_day_hospitalized,
                            # breastfing 
                            case
								when obs regexp "!!1151=(5526)!!" then @breastfing:="Breastfed exclusively (EBF)"
                                when obs regexp "!!1151=(1708)!!" then @breastfing:="Exclusive Replacement Feeding (ERF)"
                                when obs regexp "!!1151=(6046)!!" then @breastfing:="Mixed Feeding (MF)"
								else @breastfing:= null
							end as breastfing,
                            #at_risk_population
                            case
								when obs regexp "!!6181=(6180)!!" then @at_risk_population:="Client to sex worker (cSW)"
                                when obs regexp "!!6181=(6090)!!" then @at_risk_population:="Discorded couple (DC)"
                                when obs regexp "!!6181=(1505)!!" then @at_risk_population:="Fisher folk (FF)"
								when obs regexp "!!6181=(105)!!" then @at_risk_population:="Injection drug user (IDU)"
								when obs regexp "!!6181=(6179)!!" then @at_risk_population:="Same sex partnership (SSP/MSM)"
								when obs regexp "!!6181=(6177)!!" then @at_risk_population:="Sex worker (SW)"
								when obs regexp "!!6181=(6178)!!" then @at_risk_population:="Truck driver (TD)"
								when obs regexp "!!6181=(1175)!!" then @at_risk_population:="N/A"
								else @at_risk_population:= null
							end as at_risk_population,
                            #pwp Disclosure
                            case
								when obs regexp "!!1048=(1065)!!" then @pwp_disclosure:="Yes"
                                when obs regexp "!!1048=(1066)!!" then @pwp_disclosure:="No"
                                when obs regexp "!!1048=(1175)!!" then @pwp_disclosure:="N/A"
								else @pwp_disclosure:= null
							end as pwp_disclosure,
                            #pwp_partner_tested
                            case
								when obs regexp "!!1363=(1065)!!" then @pwp_partner_tested:="Yes"
                                when obs regexp "!!1363=(1066)!!" then @pwp_partner_tested:="No"
                                when obs regexp "!!1363=(1175)!!" then @pwp_partner_tested:="N/A"
								else @pwp_partner_tested:= null
							end as pwp_partner_tested,
                            #condoms_dispensed
                            case
								when obs regexp "!!6781=(1065)!!" then @condoms_dispensed:="Yes"
                                when obs regexp "!!6781=(1066)!!" then @condoms_dispensed:="No"
                                when obs regexp "!!6781=(1175)!!" then @condoms_dispensed:="N/A"
								else @condoms_dispensed:= null
							end as condoms_dispensed,
                            #sti_screened
                             case
								when obs regexp "!!6780=(1065)!!" then @sti_screened:="Yes"
                                when obs regexp "!!6780=(1066)!!" then @sti_screened:="No"
                                when obs regexp "!!6780=(1175)!!" then @sti_screened:="N/A"
								else @sti_screened:= null
							end as sti_screened,
                            #tca
                            case
								when obs regexp "!!5096=" then @tca :=
									replace(replace((substring_index(substring(obs,locate("!!5096=",obs)),@sep,ROUND ((LENGTH(obs) - LENGTH( REPLACE ( obs, "!!5096=", "") ) ) / LENGTH("!!5096=") ))),"!!5096=",""),"!!","")
								else @tca:= null
							end as tca
                                                   
                            
                            
                            from flat_visit_summary_0 t1 
							join openmrs.person p using (person_id)
                            where encounter_type in (21,22)
						 
                            );
                            
                            replace into flat_visit_summary
							(select
                            f1.person_id,
                            f1.uuid,
                            f1.visit_id,
                            f1.encounter_id,
                            f1.encounter_datetime,
							f1.encounter_type,
							f1.is_clinical_encounter,
							f1.location_id,
                            f1.enrollment_date,
                            f1.enrollment_date,
                            f1.patient_source,
                            f1.weight,
                            f1.height,
                            f1.cur_arv_adherence,
                            f1.pregnancy,
                            f1.fpstatus,
						    f1.fpmethod,
                            f1.why_not_on_fp,
                            f1.tb_status,
                            f1.side_effects,
                            f1.who_stage,
                            f1.ctx_adherence,
                            f1.ctx_dispensed,
                            f1.inh_dispensed,
                            f1.other_medications_dispensed,
                            f1.arvs_adherence,
                            f1.why_arvs_adherence_poor,
                            f1.pmtct_ppct,
                            f1.currentregmen,
                            f1.cd4_cd4percentage,
                            f1.Hgb,
                            f1.rpr_done,
                            f1.tb_sputum_done,
                            f1.other_test_done,
                            f1.refferal_hospitalized,
                            f1.no_of_day_hospitalized,
                            f1.breastfing,
                            f1.at_risk_population,
                            f1.pwp_disclosure,
                            f1.pwp_partner_tested,
                            f1.condoms_dispensed,
                            f1.sti_screened,
                            f1.tca
                             
						  	from flat_visit_summary_1 f1
							);
						
					
					  end while;
                 select @end := now();
				 insert into etl.flat_log values (@start,@last_date_created,@table_version,timestampdiff(second,@start,@end));
				
                      
                      END $$
	DELIMITER ;

call generate_visit_summary();