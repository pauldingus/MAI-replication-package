// MASTER CODE FILE TO PRODUCE FIGS. 4-5 IN JULY 25 VERSION OF MARKET ACTIVITY PAPER

// Inputs
* CSVs with activity measures pulled from database
* monthly weather data based on ERA5, exported from GEE
* ACLED conflict data
* shapefiles identifying adm1 and adm2 regions
* shapefile identifying equal population areas

// Parameters
graph set window fontface "Arial"

* cd "~\Dropbox\MarketActivityIndex\mai_shared"
cd "C:\Users\tillmanv\Dropbox\MarketActivityIndex\mai_shared\Submissions\MktAct\NatureComm\ReplicationPackage"

global color1 "42 157 143"
global color2 "233 196 106"
global color3 "231 111 81"
global color4 "150 177 137"
global color5 "234 155 94"


// 01 Import activity data
* output: temp/activity_appended_eth.dta
run code/figures/01_importer.do

// 02 Import weather data and merge with markets
* output: temp/activityAndWeather_eth.dta
run code/figures/02_mergeWeatherData.do

// 03 Figure 4 
* panel A - name: panela
run code/figures/03_figure4a.do

* panel B - name: seasonality
run code/figures/03_figure4b.do

* panel C - name: seasons_per_adm
run code/figures/03_figure4c.do

* panel D - name: shocks
run code/figures/03_figure4d.do

graph combine panela seasonality seasons_per_adm shocks,  altshrink graphregion(margin(none)) imargin(medsmall) name(figure4, replace)
graph display, xsize(16) ysize(16)
graph export "graphs/figure4.png", replace height(2000)

// 04 Figure 5
run code/figures/04_figure5.do

// 05 Figure S6
run code/figures/05_figure_S6.do

// 05 Figure S7
run code/figures/05_figure_S7.do
