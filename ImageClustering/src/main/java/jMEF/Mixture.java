package jMEF;

import com.ic.hbase.domain.ClusterObject;
import jMEF.Clustering.CLUSTERING_TYPE;
import java.util.List;

/**
 *
 * @author phoenix
 */
public class Mixture {

    public static MixtureModel mergeGMM1(MixtureModel mm1, MixtureModel mm2, double[] w) {
        // Variables
        int n = 0;
        n = mm1.size;
        if (n != mm2.size) {
            return null;
        }
        // Fusion the two mixture models
        MixtureModel mix = new MixtureModel(n);
        mix.EF = mm1.EF;
        int i;
        for (i = 0; i < n; i++) {
            mix.param[i] = mm1.param[i].Times(w[0]).Plus(mm2.param[i].Times(w[1]));
            mix.weight[i] = w[0] * (mm1.weight[i]) + w[1] * (mm2.weight[i]);
        }
        mix.normalizeWeights();
        // Return the mixture model
        return mix;
    }

    public static MixtureModel mergeGMM(MixtureModel mm1, MixtureModel mm2, double[] w) {
        // Variables
        int n1 = 0, n2 = 0;

        n1 = mm1.size;
        n2 = mm2.size;

        // Fusion the two mixture models
        MixtureModel mix = new MixtureModel(n1 + n2);
        mix.EF = mm1.EF;
        int i;
        for (i = 0; i < n1; i++) {
            mix.param[i] = mm1.param[i];
            mix.weight[i] = w[0] * (mm1.weight[i]);
        }
        for (i = 0; i < n2; i++) {
            mix.param[n1 + i] = mm2.param[i];
            mix.weight[n1 + i] = w[1] * mm2.weight[i];
        }

        //mix = BregmanHardClustering.simplify(mix, n1+n2, CLUSTERING_TYPE.SYMMETRIC);
        mix.normalizeWeights();
        // Return the mixture model
        return mix;
    }

    public static MixtureModel mergeManyGMM(List<ClusterObject> clusterObjectList, double[] weight) {
        MixtureModel firstMM = clusterObjectList.get(0).getGmm();
        int size = firstMM.size;

        // Fusion the multiple mixture models
        MixtureModel mix = new MixtureModel(size);
        mix.EF = firstMM.EF;

        for (int k = 0; k < size; k++) {
            mix.param[k] = firstMM.param[k].Times(weight[0]);
            mix.weight[k] = weight[0] * (firstMM.weight[k]);
        }


        for (int k = 1; k < weight.length; k++) {
            for (int i = 0; i < size; i++) {
                MixtureModel mm = clusterObjectList.get(k).getGmm();
                (mix.param[i]).Plus(mm.param[i].Times(weight[k]));
                mix.weight[i] += weight[k] * (mm.weight[i]);
            }
        }

        mix.normalizeWeights();
        return mix;
    }
}
