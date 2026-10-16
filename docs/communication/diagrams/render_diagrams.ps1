# render_diagrams.ps1
# Pré-rend les diagrammes Mermaid en PNG pour les publications.
# Prérequis : Node.js + mmdc installé globalement
#   npm install -g @mermaid-js/mermaid-cli

$OUT = "$PSScriptRoot\png"
New-Item -ItemType Directory -Force -Path $OUT | Out-Null

$diagrams = @(
    @{ src = "fig1_medallion.mmd";    out = "fig1_medallion.png";    w = 1000 },
    @{ src = "fig2_pipeline.mmd";     out = "fig2_pipeline.png";     w = 800  },
    @{ src = "fig3_infrastructure.mmd"; out = "fig3_infrastructure.png"; w = 1000 },
    @{ src = "fig4_imputation.mmd";   out = "fig4_imputation.png";   w = 900  },
    @{ src = "fig5_gsbpm.mmd";        out = "fig5_gsbpm.png";        w = 1100 }
)

foreach ($d in $diagrams) {
    $src = "$PSScriptRoot\$($d.src)"
    $out = "$OUT\$($d.out)"
    Write-Host "Rendu : $($d.src) → $($d.out)"
    mmdc -i $src -o $out -w $($d.w) -b white
}

Write-Host ""
Write-Host "Terminé. Images dans : $OUT"
Write-Host "Les PNG peuvent etre utilises pour publier le billet_pipeline_statistique.md."
