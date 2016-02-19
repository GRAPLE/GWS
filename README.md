# GrapleWebService
WebService implemented using Flask to serve as front-end to grapleR


########### description of Sample distribution jobs ###################

########## Input ##################################

1. base_file -> csv file containing fields which you want to vary.
2. driver_file -> nml file configuring the simulation eg. glm2.nml
3. job description file -> This file describes the actions that should be
   taken to generate new simulations from the base_file.
   
Note:
1. At this time only one base_file can be specified.
2. job description file should always be named "job_desc.csv"
3. In job_desc.csv file there should not be any gap line between any two
   consecutive lines, also the file should NOT end with a empty line.
   
Below we describe the "job_desc.csv" file -->

a sample file-->

$ cat job_desc.csv 
base_file,met_hourly.csv
samples,30
AirTemp,add,uniform,-40,40 <-------- First description row
RelHum,add,normal,-10,10 <-------- Last description row
<---------- No new line here

Now let us look at the parameters-
1. base_file -> name of csv file containing fields which you want to vary. 
2. samples -> number of iterations i.e. simulations you wantto generate from
the base_file.
3. Row 3 onwards -- description row
Here you must specify the field_name, mathematical operation, distri-
-bution, parameters for generating the distribution.

<field_name>,<mathematical_operation>,<distribution>,<parameters for distribution>

please note that they are comma separated. You can have any number of fields that
you may want to vary.

Inner working--

To generate the simulatons GWS will create a list containing <samples> number of
elements drawn from the defined distribution specified in each of the description 
rows.

Than for every new simulation it would select in order a value from those lists and
apply that value according to the defined mathematical operation to the original
value of the field.

for the above job_desc file 30 simulations will be created, a sim_summary.csv file
is included with the output that describes the actions that were taken to generate 
the particular simulation.

Here is a sample file--

cat sim_summary.csv 
sim_1,AirTemp,uniform,add,-15.6955434694,RelHum,normal,add,-13.3720240246
sim_2,AirTemp,uniform,add,38.371283777,RelHum,normal,add,-12.2825912618
sim_3,AirTemp,uniform,add,26.7435394937,RelHum,normal,add,-11.9553825068
sim_4,AirTemp,uniform,add,-10.0028900898,RelHum,normal,add,-4.07008470592
sim_5,AirTemp,uniform,add,31.8204457381,RelHum,normal,add,-9.30904370144
sim_6,AirTemp,uniform,add,-20.2088798594,RelHum,normal,add,-5.3078550033
sim_7,AirTemp,uniform,add,21.0997369051,RelHum,normal,add,-12.8021625931
sim_8,AirTemp,uniform,add,33.8486570241,RelHum,normal,add,-1.15951549705
sim_9,AirTemp,uniform,add,-3.46571582348,RelHum,normal,add,-13.4475311663
sim_10,AirTemp,uniform,add,-9.13901906203,RelHum,normal,add,-8.37326579202
sim_11,AirTemp,uniform,add,39.9004646953,RelHum,normal,add,-17.7981155055
sim_12,AirTemp,uniform,add,29.0067283657,RelHum,normal,add,-17.9860949242
sim_13,AirTemp,uniform,add,13.4451189739,RelHum,normal,add,-3.67215866635
sim_14,AirTemp,uniform,add,7.30034917369,RelHum,normal,add,-12.6532727825
sim_15,AirTemp,uniform,add,36.1550339765,RelHum,normal,add,4.5757527453
sim_16,AirTemp,uniform,add,-14.4753366087,RelHum,normal,add,-13.8353231746
sim_17,AirTemp,uniform,add,-14.9918187454,RelHum,normal,add,-13.2584030376
sim_18,AirTemp,uniform,add,-15.9012436415,RelHum,normal,add,10.8320784805
sim_19,AirTemp,uniform,add,18.0671146648,RelHum,normal,add,-8.37635199393
sim_20,AirTemp,uniform,add,22.3399381114,RelHum,normal,add,11.9736817858
sim_21,AirTemp,uniform,add,-3.90188641941,RelHum,normal,add,-26.1268087141
sim_22,AirTemp,uniform,add,-24.638640522,RelHum,normal,add,4.46733711485
sim_23,AirTemp,uniform,add,2.72029725386,RelHum,normal,add,-25.9117294606
sim_24,AirTemp,uniform,add,-17.2621878971,RelHum,normal,add,-22.9347163075
sim_25,AirTemp,uniform,add,1.92845701372,RelHum,normal,add,-8.48363416506
sim_26,AirTemp,uniform,add,-24.3076826598,RelHum,normal,add,-16.266435649
sim_27,AirTemp,uniform,add,-39.6413832161,RelHum,normal,add,-13.00974065
sim_28,AirTemp,uniform,add,37.6162139271,RelHum,normal,add,-9.52727038221
sim_29,AirTemp,uniform,add,-10.7423787964,RelHum,normal,add,-8.42792674723
sim_30,AirTemp,uniform,add,-27.5942637948,RelHum,normal,add,1.21956015912

Reading the above file- for sim_1, 
1. element -15.69 was drawn from uniform distribution and added to the original value of AirTemp.
2. element -13.37 was drawn from normal distribution and added to the original value of RelHum.

################################################################################
        Parameters for distributions
################################################################################

1. Uniform distribution

a. lower boundary of the interval.
b. higher boundary of the interval

2. Normal distribution

a. mean
b. standard deviation

3.  Binomial distribution

a. number of trials
b. probability of success

4. Poisson

a. expected value from interval
b. same as <samples>

returns-> a list of size <samples> containing drawn samples as per distribution.

Notes-> size in all cases is value defined in <samples>
