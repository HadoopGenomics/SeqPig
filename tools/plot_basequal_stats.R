library(rgl)
z = matrix(scan("basequal_stats.txt"),nrow=100, ncol=47, byrow=TRUE)
x <- (1:nrow(z)) # 10 meter spacing (S to N)
y <- (1:ncol(z)) # 10 meter spacing (E to W)
zlim <- range(z)
zlen <- zlim[2] - zlim[1] + 1
colorlut <- terrain.colors(zlen,alpha=0) # height color lookup table
col <- colorlut[ z-zlim[1]+1 ] # assign colors to heights for each point
open3d()
rgl.surface(x, y, z, color=col, alpha=0.75, back="lines")
axes3d(
    edges=c('x--', 'y+-', 'z--'),
    labels=T
)
