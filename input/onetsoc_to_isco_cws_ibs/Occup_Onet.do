*Using the output from Tyler to merge the Onet database 27.3 version of language component

if "`c(username)'"=="LukasDelgado" {
global main "/Users/LukasDelgado/Desktop/Research/Immigration Turkey:Colombia"
global proc "$main/Occupation GEIH"
global lang "$main/Language"
}

*First import the csv file from Tyler that contains the crosswalk from cno70 to isco-08
import delimited using "$proc/co_tk/output/cw_cno70_isco08.csv", clear

preserve
keep cno70 isco08_l2 isco08
tempfile cno70_isco08
save `cno70_isco08'
restore 

drop v1 isco08_desc cno70 cno70_desc major_group skill_level

*Merge with the codes from ONET and ISCO-08
joinby isco08 using "$proc/co_tk/input/onetsoc_to_isco_cws_ibs/soc10_isco08.dta"

*Merge with the codes from ONET SOC 10 to ONET SOC 00
rename soc10 soc2010
joinby soc2010 using "$proc/co_tk/input/onetsoc_to_isco_cws_ibs/soc00_soc10.dta"

*Merge with the codes from ONET SOC 00 to ISCO-88
rename soc2000 soc00
joinby soc00 using "$proc/co_tk/input/onetsoc_to_isco_cws_ibs/isco88_soc00.dta"
destring isco88, replace

*Rename and drop not needed variables
rename soc2010 soc10
keep isco08 isco08_l2 soc10 isco88

tempfile occup_codes
save `occup_codes'

*Take the data of Skills from ONET
import excel "$proc/Onet/Skills_27.3v.xlsx", first clear

rename *, lower 

//keep only the needed measurements for language component
keep if elementname=="Reading Comprehension" | elementname=="Active Listening" | elementname=="Writing" | elementname=="Speaking"

keep onetsoccode elementname scaleid datavalue

//simplify values and names
rename datavalue score
replace elementname=subinstr(elementname, " ", "", 5) 

//reshape so that each ONET-SOC code has one observation with all task measures */
reshape wide score, i(onetsoccode elementname) j(scaleid) string

//collapse at the ONET-SOC code level
collapse (mean) lang_imp=scoreIM lang_lev=scoreLV, by(onetsoccode)

//final cleaning
sort onetsoccode
rename onetsoccode onetsoc10

label var lang_imp "Importance average of reading, listening, writing and speaking"
label var lang_lev "Level average of reading, listening, writing and speaking"

	replace onetsoc10 = subinstr(onetsoc10, "-", "", 1)
	destring onetsoc10, replace
	gen soc10=int(onetsoc10)

*Then merge with the isco codes
joinby soc10 using `occup_codes'

order onetsoc10 soc10 isco08 isco08_l2 isco88 lang_imp lang_lev

save "$lang/lang_occup_isco_soc.dta", replace

*tostring isco08, replace
*gen isco08_l2=substr(isco08,1,2)
*destring isco08 isco08_l2, replace

joinby isco08_l2 using `cno70_isco08'

*In cno-70 you have multiple codes for each isco, so we do the weighted average 
collapse (mean) lang_imp lang_lev, by(cno70) //I'm losing the oficio 30 with this collapse

rename cno70 oficio 

save "$lang/col_lang_occup.dta", replace
