[CmdletBinding()]
param([Parameter(Mandatory)][string]$SourceImage)
Set-StrictMode -Version Latest
$ErrorActionPreference = 'Stop'
Add-Type -AssemblyName System.Drawing
$destination = Join-Path (Split-Path $PSScriptRoot -Parent) 'apps/ui/desktop/assets'
[IO.Directory]::CreateDirectory($destination) | Out-Null
$sourceStream = [IO.MemoryStream]::new([IO.File]::ReadAllBytes((Resolve-Path -LiteralPath $SourceImage)))
$source = [Drawing.Image]::FromStream($sourceStream)
try {
    if ($source.Width -ne $source.Height) {
        throw 'SourceImage must be the square application icon, not the full brand reference sheet.'
    }
    $frames = @()
    foreach ($size in @(16, 20, 24, 32, 40, 48, 64, 128, 256)) {
        $bitmap = [Drawing.Bitmap]::new($size, $size)
        $graphics = [Drawing.Graphics]::FromImage($bitmap)
        try {
            $graphics.InterpolationMode = [Drawing.Drawing2D.InterpolationMode]::HighQualityBicubic
            $graphics.PixelOffsetMode = [Drawing.Drawing2D.PixelOffsetMode]::HighQuality
            $graphics.DrawImage($source, 0, 0, $size, $size)
            $stream = [IO.MemoryStream]::new()
            try {
                $bitmap.Save($stream, [Drawing.Imaging.ImageFormat]::Png)
                $frames += ,@{ Size = $size; Bytes = $stream.ToArray() }
                if ($size -eq 256) { [IO.File]::WriteAllBytes((Join-Path $destination 'venue.png'), $stream.ToArray()) }
            } finally { $stream.Dispose() }
            if ($size -eq 64) {
                $rgba = [Collections.Generic.List[byte]]::new()
                for ($y = 0; $y -lt $size; $y++) {
                    for ($x = 0; $x -lt $size; $x++) {
                        $pixel = $bitmap.GetPixel($x, $y)
                        $rgba.AddRange([byte[]]@($pixel.R, $pixel.G, $pixel.B, $pixel.A))
                    }
                }
                [IO.File]::WriteAllBytes((Join-Path $destination 'venue-64.rgba'), $rgba.ToArray())
            }
        } finally { $graphics.Dispose(); $bitmap.Dispose() }
    }
    $file = [IO.File]::Create((Join-Path $destination 'venue.ico'))
    $writer = [IO.BinaryWriter]::new($file)
    try {
        $writer.Write([uint16]0); $writer.Write([uint16]1); $writer.Write([uint16]$frames.Count)
        $offset = 6 + 16 * $frames.Count
        foreach ($frame in $frames) {
            $dimension = [byte]($frame.Size % 256)
            $writer.Write($dimension); $writer.Write($dimension)
            $writer.Write([byte]0); $writer.Write([byte]0)
            $writer.Write([uint16]1); $writer.Write([uint16]32)
            $writer.Write([uint32]$frame.Bytes.Length); $writer.Write([uint32]$offset)
            $offset += $frame.Bytes.Length
        }
        foreach ($frame in $frames) { $writer.Write([byte[]]$frame.Bytes) }
    } finally { $writer.Dispose() }
} finally { $source.Dispose(); $sourceStream.Dispose() }
Write-Output "VENUE icons exported to $destination"
