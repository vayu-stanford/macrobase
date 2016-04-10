package macrobase.analysis.stats;

import macrobase.analysis.stats.distribution.MultivariateTDistribution;
import macrobase.conf.MacroBaseConf;
import macrobase.conf.MacroBaseDefaults;
import macrobase.datamodel.Datum;
import macrobase.util.AlgebraUtils;
import org.apache.commons.math3.linear.*;
import org.apache.commons.math3.special.Gamma;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;

/**
 * Fit Gaussian Mixture models using Variational Bayes
 */
public class VariationalGMM extends BatchMixtureModel {
    private static final Logger log = LoggerFactory.getLogger(VariationalGMM.class);

    private int K;  // Number of mixture components
    private double progressCutoff;

    private double priorAlpha;
    private double priorBeta;
    private RealVector priorM;
    private double priorNu;
    private RealMatrix priorW;
    private double[] alpha;
    private List<MultivariateTDistribution> predictiveDistributions;

    private double[] clusterWeight;  // N_k (Bishop)

    // Gaussian-Wishart distribution coefficients
    // Coefficients for K Gaussian distributions
    private List<RealVector> m;
    private double[] beta;
    // Wishart distribution coefficients for Precision matrix (lambda)
    private List<RealMatrix> W;
    private double[] nu;



    public VariationalGMM(MacroBaseConf conf) {
        super(conf);
        this.K = conf.getInt(MacroBaseConf.NUM_MIXTURES, MacroBaseDefaults.NUM_MIXTURES);
        this.progressCutoff = conf.getDouble(MacroBaseConf.EM_CUTOFF_PROGRESS, MacroBaseDefaults.EM_CUTOFF_PROGRESS);
        log.debug("created Gausiann MM with {} mixtures", this.K);
    }

    @Override
    public void train(List<Datum> data) {
        int N = data.size();
        int dimensions = data.get(0).getMetrics().getDimension();

        //priorAlpha = 1. / dimensions;
        priorAlpha = 0.1;
        //priorBeta = 1. / dimensions;
        priorBeta = 0.1;
        priorM = new ArrayRealVector(dimensions);
        // priorNu = 1. / dimensions;
        priorNu = 0.1;
        priorW = MatrixUtils.createRealIdentityMatrix(dimensions);
        RealMatrix priorWInverse = AlgebraUtils.createInverseMatrix(priorW);

        alpha = new double[K];
        beta = new double[K];
        nu = new double[K];
        W = new ArrayList<>(K);

        // Initialize
        m = gonzalezInitilizeMixtureCenters(data, K);
        for (int k=0; k<this.K; k++) {
            alpha[k] =  1. / K;
            beta[k] = priorBeta;
            nu[k] = priorNu;
            W.add(priorW);
        }
        log.debug("initialized cluster centers as: {}", m);

        List<RealVector> clusterMean;
        List<RealMatrix> S;
        // density of each point with respect to each mixture component.
        double[][] r = new double[N][K];

        double logLikelihood = -Double.MAX_VALUE;
        for (int iteration = 0 ; ; iteration++) {
            clusterWeight = new double[K];
            double[] ex_ln_phi = new double[K];
            double[] ex_ln_det_lambda = new double[K];
            double sum_alpha = 0;
            clusterMean = new ArrayList<>(K);
            S = new ArrayList<>(K);

            // 1. calculate expectation of densities of each point coming from individual clusters - r[n][k]
            for (int k=0; k<this.K; k++) {
                sum_alpha += alpha[k];
                clusterMean.add(new ArrayRealVector(dimensions));
                S.add(new BlockRealMatrix(dimensions, dimensions));
            }

            for (int k=0; k<this.K; k++) {
                ex_ln_phi[k] = Gamma.digamma(alpha[k] - Gamma.digamma(sum_alpha));
                ex_ln_det_lambda[k] = dimensions * Math.log(2) + Math.log((new EigenDecomposition(W.get(k))).getDeterminant());
                for (int i=0; i<dimensions; i++) {
                    ex_ln_det_lambda[k] += Gamma.digamma((nu[k] - i) / 2);
                }
            }

            double _const = 0.5 * dimensions * Math.log(2 * Math.PI);
            double ex_ln_xmu;
            for (int n=0; n<N; n++) {
                double normalizingConstant = 0;
                for (int k=0; k<this.K; k++) {
                    RealVector _diff = data.get(n).getMetrics().subtract(m.get(k));
                    ex_ln_xmu = dimensions / beta[k] + nu[k] * _diff.dotProduct(W.get(k).operate(_diff));
                    r[n][k] = Math.exp(ex_ln_phi[k] + 0.5 * ex_ln_det_lambda[k] - _const - 0.5 * ex_ln_xmu);
                    normalizingConstant += r[n][k];
                }
                for (int k=0; k<this.K; k++) {
                    r[n][k] /= normalizingConstant;
                    // Calculate unnormalized cluster weight, cluster mean
                    clusterWeight[k] += r[n][k];
                    clusterMean.set(k, clusterMean.get(k).add(data.get(n).getMetrics().mapMultiply(r[n][k])));
                }
            }


            // 2. Reevaluate clusters based on densities that we have for each point.
            for (int k=0; k<this.K; k++) {
                clusterMean.set(k, clusterMean.get(k).mapDivide(clusterWeight[k]));
                for (int n=0; n<N; n++) {
                    RealVector _diff = data.get(n).getMetrics().subtract(clusterMean.get(k));
                    S.set(k, S.get(k).add(_diff.outerProduct(_diff).scalarMultiply(r[n][k])));
                }
                S.set(k, S.get(k).scalarMultiply(1 / clusterWeight[k]));
            }

            for (int k=0; k<this.K; k++) {
                alpha[k] = priorAlpha + clusterWeight[k];
                beta[k] = priorBeta + clusterWeight[k];
                m.set(k, priorM.mapMultiply(priorBeta).add(clusterMean.get(k).mapMultiply(clusterWeight[k])).mapDivide(beta[k]));
                nu[k] = priorNu + 1 + clusterWeight[k];
                RealVector adjastedMean = clusterMean.get(k).subtract(priorM);
                RealMatrix wInverse = priorWInverse.add(
                        S.get(k).scalarMultiply(clusterWeight[k])).add(
                        adjastedMean.outerProduct(adjastedMean).scalarMultiply(priorBeta * clusterWeight[k] / (priorBeta + clusterWeight[k])));
                W.set(k, AlgebraUtils.createInverseMatrix(wInverse));
            }


            predictiveDistributions = new ArrayList<>(K);
            for (int k=0; k<this.K; k++) {
                double scale = (nu[k] + 1 - dimensions) * beta[k] / (1 + beta[k]);
                RealMatrix ll = AlgebraUtils.createInverseMatrix(W.get(k).scalarMultiply(scale));
                // TODO: MultivariateTDistribution should support real values for 3rd parameters
                predictiveDistributions.add(new MultivariateTDistribution(m.get(k), ll, (int) (nu[k] - 1 - dimensions)));
            }

            log.debug("cluster means are at {}", clusterMean);
            log.debug("cluster weights are at {}", clusterWeight);

            double oldLogLikelihood = logLikelihood;
            logLikelihood = 0;
            for (int n = 0; n < N; n++) {
                logLikelihood += Math.log(score(data.get(n)));
            }

            log.info("log likelihood after iteration {} is {}", iteration, logLikelihood);

            double improvement = (logLikelihood - oldLogLikelihood) / ( -logLikelihood);
            if (improvement >= 0 && improvement < this.progressCutoff) {
                log.debug("Breaking because imporovemnt was {} percent", improvement * 100);
                break;
            } else {
                log.debug("improvement is : {}%", improvement * 100);
            }
        }
    }

    @Override
    public double score(Datum datum) {
        double density = 0;
        double sum_alpha = 0;
        for (int k=0; k<this.K; k++) {
            // If the mixture is very improbable, skip.
            if (Math.abs(alpha[k] - priorAlpha) < 1e-4) {
                continue;
            }
            sum_alpha += alpha[k];
            density += alpha[k] * this.predictiveDistributions.get(k).density(datum.getMetrics());
        }
        return density / sum_alpha;
    }

    @Override
    public double getZScoreEquivalent(double zscore) {
        return 0;
    }

    public List<RealVector> getMeans() {
        return m;
    }

    public List<RealMatrix> getCovariances() {
        List<RealMatrix> covariances = new ArrayList<>(K);
        for (int k=0; k<this.K; k++) {
            covariances.add(AlgebraUtils.createInverseMatrix(W.get(k).scalarMultiply(nu[k])));
        }
        return covariances;
    }

    public double[] getPriorAdjustedWeights() {
        return clusterWeight;
    }
}
