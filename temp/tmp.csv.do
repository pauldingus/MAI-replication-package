insheet using temp/tmp.csv.csv

twoway (scatter activity_harmonized_2018 gs_rain_shock, mcolor(42) lcolor(233)) (function 0*x^2+4.845603214840969*x+102.9594716350741, range(-1.823148965835571 2.286812305450439) lcolor(233)), graphregion(fcolor(white))  xtitle(gs_rain_shock) ytitle(activity_harmonized_2018) legend(off order()) name(gadm, replace) nodraw
