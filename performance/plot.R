# File: plot.R
# Description: Plotting pycollector performance tests.
#              It is supposed to be called from plot.sh

data <- read.csv(file="output.csv", sep=",", head=TRUE)


for (column in names(data)) {
    if (column != "Time" && column != "PID" ) {
        plot(data[,c("Time", column)], type="o", col="blue")

    }
}
