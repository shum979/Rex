package rex.recon

import rex.core.common.ManagedDataset
import rex.core.xml.ReconciliationType
import rex.recon.newimplement.ReconWithCogroup

/**
  * Created by visrai on 2/22/2018.
  * Single entry point to call reconcile on supplied left and right dataset
  * ReconConfig is recon config detail from workflow xml file
  */
class Reconciler(reconConfig: ReconciliationType) extends Serializable {

  def recon(leftDataSet: ManagedDataset, rightDataSet: ManagedDataset): ManagedDataset = {
    ReconWithCogroup(reconConfig).reconcile(leftDataSet, rightDataSet)
  }

}

object Reconciler {
  def apply(reconConfig: ReconciliationType): Reconciler = new Reconciler(reconConfig: ReconciliationType)
}
