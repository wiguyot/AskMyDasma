# Acknowledgments and Algorithm References

## Project

AskMyDasma was created by **William Guyot-Lénat** as part of his work at
LIMOS / ISIMA / INP / Université Clermont Auvergne, in close collaboration
with [**Professor Engelbert Mephu
Nguifo**](https://limos.fr/detailperson/60). The project brings together
algorithms and tools developed through research at LIMOS, alongside established
methods from the wider research community. It builds on work by many
early-career researchers and interns, whose contributions we acknowledge
below.

## Acknowledgments

We thank the current and former members of the DASMA project team for their
respective contributions to the project and its research community. The names
below are taken from the [public DASMA team page](https://dasma.limos.fr/membres.html).
This acknowledgment is collective; it does not assign a specific contribution
to any person in this repository.

- Engelbert Mephu Nguifo
- Farouk Toumani
- Florentin Jiechieu
- Anthony Bertrand
- Raïssa Mbiadou Saleu
- Abdoul Jalil Djiberou Mahamadou
- Michael F. Mbouopda
- Anne Marthe Ngo Bibinbe
- Vasilis Cako
- Mohamed Rayane Bakari
- Kevin Dilan Nguetche
- Ilyes Nourine
- Marie Devita
- Julien Bounouar
- Stéphane Marchal
- Amokrane Hadj Kaddour

We also acknowledge **Clémence Audrey Emaha Tchendjam** for proposing
mDragStream, a multivariate extension of DragStream, as part of her 2025 Master's
work at ISIMA, Université Clermont Auvergne. Her work cites **Engelbert Mephu
Nguifo, Norbert Tsopze, and Patrick Asante Owusu** as supervisors. The project
uses the Point-based mDragStream variant. This attribution is based on the
available Master's-work and author information; it does not imply that the
work is a peer-reviewed paper. A stable public thesis record should be added
when one is available.

## Scientific references

These references describe the published algorithms that inspired or are used
by the project. They do not imply that this repository is the original
implementation or that its runtime matches every detail of each paper.

1. **HSTree / Half-Space Trees** — Swee Chuan Tan, Kai Ming Ting, and Fei Tony
   Liu. “Fast Anomaly Detection for Streaming Data.” *Proceedings of the 22nd
   International Joint Conference on Artificial Intelligence (IJCAI 2011)*,
   pp. 1511–1516, 2011. [DOI: 10.5591/978-1-57735-516-8/IJCAI11-254](https://doi.org/10.5591/978-1-57735-516-8/IJCAI11-254).
   This is the reference for the Paper variant. The project also provides a
   PySAD variant; see the [HSTree differences note](../models/hstree/paper-deviations.md).

2. **Isolation Forest (iForest)** — Fei Tony Liu, Kai Ming Ting, and Zhi-Hua
   Zhou. “Isolation Forest.” *2008 Eighth IEEE International Conference on
   Data Mining*, pp. 413–422, 2008. [DOI: 10.1109/ICDM.2008.17](https://doi.org/10.1109/ICDM.2008.17).

3. **KitNET** — Yisroel Mirsky, Tomer Doitshman, Yuval Elovici, and Asaf
   Shabtai. “Kitsune: An Ensemble of Autoencoders for Online Network Intrusion
   Detection.” *Network and Distributed System Security Symposium (NDSS)*,
   2018. [DOI: 10.14722/ndss.2018.23204](https://doi.org/10.14722/ndss.2018.23204).
   KitNET is the anomaly-detection algorithm described in the Kitsune paper;
   this project uses the detector without the full Kitsune network pipeline.

4. **DeepAnT** — Mohsin Munir, Muhammad Shoaib Ahmed Siddiqui, Andreas Dengel,
   and Sheraz Ahmed. “DeepAnT: A Deep Learning Approach for Unsupervised
   Anomaly Detection in Time Series.” *IEEE Access*, vol. 7, pp. 1991–2005,
   2019. [DOI: 10.1109/ACCESS.2018.2886457](https://doi.org/10.1109/ACCESS.2018.2886457).

5. **DragStream (univariate)** — Anne Marthe Sophie Ngo Bibinbe, Abdoul Jalil
   Djiberou Mahamadou, Michael Franklin Mbouopda, and Engelbert Mephu Nguifo.
   “DragStream: An Anomaly and Concept Drift Detector in Univariate Data
   Streams.” *2022 IEEE International Conference on Data Mining Workshops
   (ICDMW)*, pp. 842–851, 2022. [DOI:
   10.1109/ICDMW58026.2022.00113](https://doi.org/10.1109/ICDMW58026.2022.00113).
   This is the original univariate algorithm, not the multivariate mDragStream
   extension.

6. **mDragStream (multivariate)** — Clémence Audrey Emaha Tchendjam. *Multivariate
   DragStream: An Anomaly and Concept Drift Detection on Multivariate Data
   Streams*. Master's thesis, ISIMA, Université Clermont Auvergne, 2025.
   [Author's public post about mDragStream](https://www.linkedin.com/posts/cl%C3%A9mence-audrey-emaha-tchendjam_anomalydetection-datastreams-realtimesystems-activity-7401652807197499393-FObf).
   We have not found a peer-reviewed publication specifically about this
   multivariate extension. Cite the thesis as Master's work, not as an
   additional peer-reviewed paper.
