use "temp/activityAndWeather_eth.dta", replace
keep if inrange(date, td(01jul2017), td(01mar2020))&  mktday==1

gen monthly_date =mofd(date)
gen weekly_date =wofd(date)

gen markets_per_adm2 = .
levelsof admlvl2, local(adms)
foreach adm of local adms{
	qui tab mktid if admlvl2=="`adm'"
	qui replace markets_per_adm2 = r(r)  if admlvl2=="`adm'"
}

collapse (mean) activity_harmonized_* (mean) marketlat marketlon markets_per_adm2 , by(admlvl2 weekly_date mktday year month admlvl1)

keep if markets_per_adm2>10

preserve // keep 30 adm2s
set seed 2714
	duplicates drop admlvl2, force
	gsort -markets_per_adm2
	keep if _n<=15 | admlvl2=="Central"
	keep admlvl2
	tempfile tmp
	save `tmp'
restore
merge m:1 admlvl2 using `tmp', keep(3) nogen

replace admlvl2 = "AACentral" if admlvl2=="Central"
pause on
capture drop smooth*
levelsof admlvl2, local(adms)
local comb
local opts ytitle("") xtitle("") graphregion(margin(zero) ) plotregion(margin(zero)  ) yscale(range(60(20)100))  

foreach adm of local adms{
		local a=`a'+1
		lpoly activity_harmonized_2018 weekly_date if admlvl2=="`adm'", degree(2) bw(8) nogr gen(smooth_x_`a' smooth_y_`a') n(100) name(g`a', replace)
		//replace smooth_y_`a' = . if !inrange(smooth_y_`a', tw(2018-w1), t2(2018-w52))
		qui su smooth_y_`a'  if inrange(smooth_x_`a', tw(2018-w1), tw(2018-w52))
		gen smooth_y_`a'_rescaled = 100*smooth_y_`a'/r(max)
		qui su smooth_x_`a' if inrange(smooth_y_`a', r(max)-0.000001, r(max)+0.000001)
		gen smooth_x_`a'_rescaled = smooth_x_`a'- tw(2018-w1) //r(mean)
		gen adm_`a' = "`adm'"
		
	}
	
	keep *rescaled* adm_*

	renvars , postsub("_rescaled" "")
	gen ii=_n
	reshape long smooth_x smooth_y adm, i(ii) j(adm2) string
	gen weekly_date = int(smooth_x)
	keep if inrange(smooth_x, 0,52)
	gen doy = int(smooth_x*7)
	gen date = mdy(1,1,2018) + doy - 1
	format date %td
	sort adm date
	gen month=month(date)
	rename adm admlvl2
	
	preserve
		import delimited "datasets\weather\Ethiopia_rain_byadm2_20250625.csv", encoding(ISO-8859-2) clear 
			drop geo shapeid shapeiso systemindex shapegroup shapetype
			rename shapename admlvl2
			rename mean precipitation
			replace precipitation = precipitation*1000
			replace precipitation=400 if inrange(precipitation, 400,10000)
			tostring daterain, replace
			replace daterain=daterain+"01"
			gen date=date(daterain, "YMD")
			format date %td
			gen monthly_date =mofd(date)
			format monthly_date %tm
			gen month=month(date)
			gen year=year(date)
			keep if year==2018
			replace admlvl2 = "AACentral" if admlvl2=="Central"

			tempfile weather
			save `weather'
	restore
	merge m:1 admlvl2 month using `weather', keep(3) nogen
	bysort admlvl2 month: gen count_per_month=_n
	
	gen lower=50
	su precipitation
	local max=r(max)
	gen upper=(precipitation/`max')*10+lower

	tw (line smooth_y smooth_x if inrange(smooth_x, 0,52), lc("$color1%50") lw(*4) subtitle("", fcolor(none) lstyle(none))) ///
	   (rbar lower upper smooth_x if count_per_month==1, color("gs6%50") lw(0) barw(2.5)) ///
		, by(adm2,  compact title("{bf:C}", pos(10)) note("")  legend(off) graphregion(margin(zero) )) ///
		ytitle("Market activity index" " ", size(medsmall)) xtitle("") xlabel(2.5 "Jan" 20 "May" 38.5 "Sep" , nogrid labsize(vlarge) ///
		angle(45))  ylabel(75(25)100, nogrid  labsize(vlarge) angle(0)) name(seasons_per_adm, replace) graphregion(margin(zero) ) plotregion(lc(gs10))
		
	