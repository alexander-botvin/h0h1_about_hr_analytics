### Data simulation
set.seed(2077)

n <- 1000
M <- rnorm(n)
P <- rnorm(n)
A <- rnorm(n)
X <- rnorm(n)
U <- rnorm(n)  

logit_remote <- 0.5 * pmax(A, X)
prob_remote <- 1 / (1 + exp(-logit_remote))
F <- cut(prob_remote,
         breaks = c(-Inf, 0.45, 0.76, Inf),
         labels = c("office", "hybrid", "remote"),
         right = FALSE)
F_numeric <- as.numeric(F) - 1

L <- 0.6 * M + 0.4 * P + 0.3 * A + 0.5 * F_numeric + rnorm(n, 0, 0.5)
R <- 0.6 * L + 0.6 * U + rnorm(n, 0, 0.2)
E <- 0.1 * L + 0.5 * M + 0.4 * X + 0.6 * F_numeric + 0.5 * U + rnorm(n, 0, 0.5)

# Create dataframe
df <- data.frame(Motivation = M, 
                 PeerLearningCulture = P, 
                 Autonomy = A, 
                 Experience = X, 
                 WorkFormat = F, 
                 Learning = L, 
                 ManagerRating = R, 
                 Efficiency = E
                 )

# Rescale
df$Motivation <- round(scales::rescale(df$Motivation, to = c(0, 100)),0)
df$PeerLearningCulture <- round(scales::rescale(df$PeerLearningCulture, to = c(1, 5)))
df$Autonomy <- round(scales::rescale(df$Autonomy, to = c(0, 100)))
df$Experience <- scales::rescale(df$Experience, to = c(0, 20))
df$Learning <- round(scales::rescale(df$Learning, to = c(0, 100)),0)
df$Efficiency <- scales::rescale(df$Efficiency, to = c(0, 100))
df$ManagerRating <- round(scales::rescale(df$ManagerRating, to = c(1, 5)))
