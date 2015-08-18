import ROOT
ROOT.gROOT.SetBatch(True)
ROOT.gStyle.SetOptStat(0)

with open("results.txt", "r") as f:
  results = f.readlines()

size = 0
maxDiff = 0
for line in results:
  cleanSplit = line.rstrip("\n").split(" ")
  expected = int(cleanSplit[0])
  actual = int(cleanSplit[1])
  size = max(size, expected+2, actual+2)
  maxDiff = max(maxDiff, actual-expected)

hist = ROOT.TH2F("Path Selection Frequency", "Path Selection Frequency", size, 0, size, size, 0, size)
hist.GetXaxis().SetTitle("Shortest Path Length")
hist.GetYaxis().SetTitle("Selected Path Length")

hist2 = ROOT.TH1F("Selected Paths Extra Hops", "Selected Paths Extra Hops", maxDiff+1, 0, maxDiff+1)

for line in results:
  cleanSplit = line.rstrip("\n").split(" ")
  expected = int(cleanSplit[0])
  actual = int(cleanSplit[1])
  count = int(cleanSplit[2])
  _ = hist.SetBinContent(expected+1, actual+1, count)
  _ = hist2.Fill(actual-expected, count)

canvas = ROOT.TCanvas()
canvas.SetLogz(1)
hist.Draw("COLZ")
canvas.Print("test.pdf")

canvas2 = ROOT.TCanvas()
canvas2.SetLogy(1)
hist2.Draw()
canvas2.Print("test2.pdf")

